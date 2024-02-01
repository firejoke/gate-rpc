# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/3/30 11:09
import asyncio
import contextvars
import functools
import inspect
import sys
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Coroutine, Callable, AsyncGenerator, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from time import time
from traceback import format_exception, format_tb
from typing import Annotated, List, Union, overload, Optional
from uuid import uuid4
from logging import getLogger

import zmq.constants as z_const
import zmq.asyncio as z_aio
from zmq.auth import Authenticator

from .global_settings import Settings
from .exceptions import (
    BuysWorkersError, HugeDataException, RemoteException,
    ServiceUnAvailableError,
)
from .mixins import _LoopBoundMixin
from .utils import (
    BoundedDict, HugeData, StreamReply, check_socket_addr, check_zap_args,
    generate_reply,
    interface, msg_pack, msg_unpack, from_bytes, to_bytes,
)

__all__ = ["Worker", "Service", "AsyncZAPService", "AMajordomo", "Client"]


logger = getLogger(__name__)


class Context(z_aio.Context):
    @overload
    def __init__(self, io_threads: int = 1):
        ...

    @overload
    def __init__(self, io_threads: "Context"):
        ...

    @overload
    def __init__(self, *, shadow: Union["Context", int]):
        ...

    def __init__(
        self, *, io_threads: Union[int, "Context"] = 1,
        shadow: Union["Context", int] = 0
    ) -> None:
        if shadow:
            super().__init__(shadow=shadow)
        else:
            super().__init__(io_threads=io_threads)


class ABCWorker(ABC):
    """
    Worker 的抽象基类，可以根据资源需求设置是否限制接收的请求数量。
    """
    identity: bytes
    service: "ABCService"
    address: Union[str, bytes]
    heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS
    heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL
    expiry: float
    max_allowed_request_queue: int = 0

    @abstractmethod
    def is_alive(self):
        pass


class ABCService(ABC):
    """
    Service 的抽象基类
    """
    name: str
    running: bool
    description: str

    @abstractmethod
    def add_worker(self, worker: Optional[ABCWorker]):
        pass

    @abstractmethod
    def remove_worker(self, worker: Optional[ABCWorker]):
        pass

    @abstractmethod
    def get_workers(self):
        pass

    @abstractmethod
    def acquire_idle_worker(self) -> Optional[ABCWorker]:
        pass

    @abstractmethod
    def release_worker(self, worker: Optional[ABCWorker]):
        pass


class RemoteWorker(ABCWorker):
    def __init__(
        self,
        identity: bytes,
        address: bytes,
        heartbeat: int,
        service: "Service"
    ) -> None:
        """
        远程 worker 的本地映射
        :param identity: 远程 worker 的id，也是地址
        :param heartbeat: 和远程 worker 的心跳间隔
        :param service: 该 worker 提供的服务的名字
        """
        self.identity = identity
        self.ready = False
        self.service = service
        self.heartbeat = heartbeat
        self.expiry = 1e-3 * self.heartbeat_liveness * self.heartbeat + time()
        self.address = address
        self.destroy_task: Optional[asyncio.Task] = None

    def is_alive(self):
        if time() > self.expiry:
            return False
        return True


class Worker(ABCWorker, _LoopBoundMixin):
    """
    所有要给远端调用的方法都需要用interface函数装饰。
    """

    def __init__(
        self,
        broker_addr: str,
        service: "Service",
        identity: str = None,
        heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        context: Context = None,
        zap_mechanism: str = None,
        zap_credentials: tuple = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ):
        """
        :param broker_addr: 要连接的代理地址
        :param service: Service 实例
        :param identity: id
        :param heartbeat: 心跳间隔，单位是毫秒
        :param context: 用于创建socket的zeroMQ的上下文
        :param zap_mechanism: 使用的 ZAP 验证机制
        :param zap_credentials: 用于 ZAP 验证的凭据
        :param thread_executor: 用于执行io密集型任务
        :param process_executor: 用于执行cpu密集型任务
        """
        if not identity:
            identity = uuid4().hex
        self.identity = f"gw-{identity}".encode("utf-8")
        self.service = service
        self.ready = False
        self._broker_addr = check_socket_addr(broker_addr)
        zap_mechanism, zap_credentials = check_zap_args(
            zap_mechanism, zap_credentials
        )
        if not zap_mechanism:
            self.zap_frames = []
        else:
            self.zap_frames = [zap_mechanism, *zap_credentials]

        if not context:
            self._ctx = Context()
        else:
            self._ctx = context
        self._socket = None
        self._poller = z_aio.Poller()
        self.heartbeat = heartbeat
        self.expiry = self.heartbeat_liveness * self.heartbeat

        self.requests = BoundedDict(
            Settings.MESSAGE_MAX, 0.1
        )

        if not thread_executor:
            thread_executor = ThreadPoolExecutor()
        self.thread_executor = thread_executor
        if not process_executor:
            process_executor = ProcessPoolExecutor()
        self.process_executor = process_executor
        self.default_executor = self.thread_executor

        self.interfaces: dict = self.get_interfaces()

        self._reconnect: bool = False
        self._recv_task: Optional[asyncio.Task] = None

    def get_interfaces(self):
        interfaces = {
            "_get_interfaces": {
                "doc": "get interfaces",
                "signature": inspect.signature(self.get_interfaces)
            }
        }
        for name, method in inspect.getmembers(self):
            if (
                    (
                            inspect.ismethod(method)
                            or inspect.iscoroutinefunction(method)
                    )
                    and not name.startswith("_")
                    and getattr(method, "__interface__", False)
            ):
                interfaces[name] = {
                    "doc": inspect.getdoc(method),
                    "signature": inspect.signature(method)
                }
        return interfaces

    def is_alive(self):
        return self.ready

    async def connect(self):
        if not self.is_alive():
            logger.info(
                f"Connecting to broker at {self._broker_addr} ..."
            )
            self._socket = self._ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
            self._socket.set_hwm(Settings.ZMQ_HWM)
            self._socket.connect(self._broker_addr)
            self._poller.register(self._socket, z_const.POLLIN)
            service = ":".join((self.service.name, self.service.description))
            await self.send_to_broker(
                Settings.MDP_COMMAND_READY,
                [service]
            )
            self.ready = True

    async def disconnect(self):
        if self.is_alive():
            logger.warning("Disconnecting from broker")
            await self.send_to_broker(
                Settings.MDP_COMMAND_DISCONNECT
            )
            self._poller.unregister(self._socket)
            self._socket.disconnect(self._broker_addr)
            self._socket.close()
            self._socket = None

    def close(self):
        """
        关闭打开的资源。
        """
        self.thread_executor.shutdown()
        self.process_executor.shutdown()

    async def stop(self):
        if self.is_alive():
            if not self._recv_task.cancelled():
                self._recv_task.cancel()
            await self.disconnect()
            self.close()
            self.ready = False

    def release_request(self, task: asyncio.Task):
        self.requests.pop(task.get_name())
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.debug(f"process request task exception:\n{exception_info}")

    async def send_to_broker(
        self,
        command: bytes,
        option: list[Union[bytes, str]] = None,
        messages: list[Union[bytes, str]] = None
    ):
        """
        在MDP的Worker端发出的消息的命令帧后面加一个worker id帧，用作掉线重连，幂等。
        """
        if not self._socket:
            return
        if not messages:
            messages = []
        if not option:
            option = []
        if self.zap_frames:
            option = self.zap_frames + option
        messages = [
            b"",
            Settings.MDP_WORKER,
            command,
            self.identity,
            *option,
            *messages
        ]
        messages = [to_bytes(s) for s in messages]
        await self._socket.send_multipart(messages)

    def run_in_executor(self, executor, func, *args, **kwargs):
        loop = self._get_loop()
        func_call = functools.partial(
            func, *args, **kwargs
        )
        if isinstance(executor, ThreadPoolExecutor):
            ctx = contextvars.copy_context()
            func_call = functools.partial(
                ctx.run, func, *args, **kwargs
            )
        return loop.run_in_executor(executor, func_call)

    async def process_request(
        self, cli_id: bytes, request_id: bytes, func_details: dict
    ):
        """
        MDP的reply命令的命令帧后加上一个worker id帧，掉线重连，幂等
        """
        loop = self._get_loop()
        try:
            try:
                func_name, args, kwargs = (
                    func_details["func"],
                    func_details["args"],
                    func_details["kwargs"]
                )
            except Exception:
                raise AttributeError("Invalid function details")
            if not self.interfaces.get(func_name, None):
                raise AttributeError(
                    "Function not found: {0}".format(func_name)
                )
            func = getattr(self, func_name)
            if inspect.iscoroutinefunction(func):
                coro = func(*args, **kwargs)
                task = loop.create_task(coro)
            else:
                task = self.run_in_executor(
                    self.default_executor, func, *args, **kwargs
                )
        except Exception as e:
            reply = await generate_reply(exception=e)
        else:
            reply = await generate_reply(task)
        option = [cli_id, request_id]
        if isinstance(reply, HugeData):
            option.append(Settings.STREAM_HUGE_DATA_TAG)
            reply_except = False
            async for data in reply.compress():
                if reply_except:
                    data = msg_pack(data)
                if data == HugeData.except_tag:
                    reply_except = True
                await self.send_to_broker(
                    Settings.MDP_COMMAND_REPLY, option, [data]
                )
            await self.send_to_broker(
                Settings.MDP_COMMAND_REPLY, option,
                [Settings.STREAM_END_MESSAGE]
            )
            reply.destroy()
        elif isinstance(reply, AsyncGenerator):
            option.append(Settings.STREAM_GENERATOR_TAG)
            async for sub_reply in reply:
                messages = [msg_pack(sub_reply)]
                await self.send_to_broker(
                    Settings.MDP_COMMAND_REPLY, option, messages
                )
            await self.send_to_broker(
                Settings.MDP_COMMAND_REPLY, option,
                [Settings.STREAM_END_MESSAGE]
            )
        else:
            messages = [msg_pack(reply)]
            await self.send_to_broker(
                Settings.MDP_COMMAND_REPLY, option, messages
            )

    async def recv(self):
        try:
            await self.connect()
            logger.info("Worker event loop starts running.")
            loop = self._get_loop()
            self.ready = True
            heartbeat_at = 1e-3 * self.heartbeat + loop.time()
            broker_liveness = self.heartbeat_liveness
            while 1:
                if not self.ready:
                    break
                socks = dict(await self._poller.poll(self.heartbeat))
                if loop.time() > heartbeat_at:
                    await self.send_to_broker(
                        Settings.MDP_COMMAND_HEARTBEAT,
                    )
                    heartbeat_at = 1e-3 * self.heartbeat + loop.time()
                if socks:
                    messages = await self._socket.recv_multipart()
                    broker_liveness = self.heartbeat_liveness
                    try:
                        (
                            empty,
                            mdp_worker_version,
                            command,
                            *messages
                        ) = messages
                        if empty:
                            continue
                    except ValueError:
                        continue
                    if mdp_worker_version != Settings.MDP_WORKER:
                        continue
                    if command == Settings.MDP_COMMAND_HEARTBEAT:
                        pass
                    elif command == Settings.MDP_COMMAND_DISCONNECT:
                        logger.debug(f"{self.identity} disconnected")
                        break
                    elif command == Settings.MDP_COMMAND_REQUEST:
                        if self.requests.usable_size == 0:
                            continue
                        try:
                            client_id, empty, request_id, body = messages
                            if empty:
                                raise ValueError
                        except ValueError:
                            continue
                        details = msg_unpack(body)
                        tn = request_id.decode("utf-8")
                        task = loop.create_task(
                            self.process_request(
                                client_id, request_id, details
                            ), name=tn
                        )
                        try:
                            await self.requests.aset(tn, task)
                        except asyncio.TimeoutError:
                            logger.error(
                                f"The number of pending requests has exceeded"
                                f" the cache limit for the requesting task."
                                f"The limit is {self.requests.maxsize} "
                                f"requests."
                            )
                        task.add_done_callback(self.release_request)
                else:
                    broker_liveness -= 1
                    if broker_liveness == 0:
                        logger.warning("Broker offline")
                        if self._reconnect:
                            logger.warning("Reconnect to Majordomo")
                            await self.disconnect()
                            self._recv_task = loop.create_task(self.recv())
                            break
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            logger.error(exception)
            raise

    def run(self):
        if not self._recv_task:
            loop = self._get_loop()
            self._recv_task = loop.create_task(self.recv())


class Service(ABCService):
    """
    管理本地和远程提供该服务的 worker。
    TODO: 如何避免有伪造的假服务连上来，会导致客户端调用本该有却没有的接口而出错。
    """

    def __init__(
        self, name: str = Settings.SERVICE_DEFAULT_NAME,
        description: str = "gate-rpc service"
    ):
        """
        :param name: 该服务的名称
        :param description: 描述该服务能提供的功能
        """
        self.name = name
        self.description: Annotated[str, "Keep it short"] = description
        self.workers: dict[bytes, ABCWorker] = dict()
        self.running = False
        self.idle_workers: deque[bytes] = deque()

    def add_worker(self, worker: ABCWorker):
        if worker.identity in self.workers:
            return
        worker.service = self
        self.workers[worker.identity] = worker
        self.idle_workers.appendleft(worker.identity)
        if not self.running:
            self.running = True

    def remove_worker(self, worker: ABCWorker):
        if worker.identity in self.workers:
            self.workers.pop(worker.identity)
        if worker.identity in self.idle_workers:
            self.idle_workers.remove(worker.identity)
        if hasattr(worker, "stop"):
            worker.stop()
        if not self.workers:
            self.running = False

    def get_workers(self) -> dict[bytes, ABCWorker]:
        return self.workers

    def acquire_idle_worker(self) -> Union[ABCWorker, None]:
        while self.idle_workers:
            worker_id = self.idle_workers.pop()
            if self.workers[worker_id].is_alive():
                return self.workers[worker_id]
        return None

    def release_worker(self, worker: ABCWorker):
        if worker.identity in self.workers and worker.is_alive():
            self.idle_workers.appendleft(worker.identity)

    def create_worker(
        self,
        worker_class: Callable[..., Worker] = None,
        workers_addr: str = None,
        workers_heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        context: Context = None,
        zap_mechanism: str = None,
        zap_credentials: tuple = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ) -> Worker:
        if not workers_addr:
            workers_addr = Settings.WORKER_ADDR
        if not worker_class:
            worker_class = Worker
        worker = worker_class(
            workers_addr,
            self,
            identity=f"{self.name}_Worker",
            heartbeat=workers_heartbeat,
            context=context,
            zap_mechanism=zap_mechanism,
            zap_credentials=zap_credentials,
            thread_executor=thread_executor,
            process_executor=process_executor
        )
        self.add_worker(worker)
        return worker


class AsyncZAPService(Authenticator, _LoopBoundMixin):
    """
    异步的 zap 身份验证服务，继承并重写为使用 ROUTE 模式
    """
    context: Context
    zap_socket: Optional[z_aio.Socket]
    encoding = "utf-8"

    def __init__(self, addr: str = Settings.ZAP_ADDR):
        super().__init__(context=Context(), log=getLogger("gaterpc.zap"))
        self.addr = addr
        self.zap_socket = self.context.socket(z_const.ROUTER, z_aio.Socket)
        # self.zap_socket.linger = 1
        self.zap_socket.bind(check_socket_addr(addr))
        self._poller = z_aio.Poller()
        self._recv_task: Optional[asyncio.Task] = None
        self._tasks = deque()

    async def handle_zap(self) -> None:
        loop = self._get_loop()
        self._poller.register(self.zap_socket, z_const.POLLIN)
        self.log.debug("ZAP Service start.")
        try:
            while 1:
                socks = dict(await self._poller.poll())
                if self.zap_socket in socks:
                    message = await self.zap_socket.recv_multipart()
                    t = loop.create_task(self.handle_zap_message(message))
                    self._tasks.append(t)
                    t.add_done_callback(self._tasks.remove)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            self.log.error(exception)
            self.stop()
            raise

    def start(self) -> None:
        loop = self._get_loop()
        if not self._recv_task:
            self.log.debug(f"create ZAP task.")
            self._recv_task = loop.create_task(self.handle_zap())

    def stop(self) -> None:
        if self._recv_task and not self._recv_task.cancelled():
            self._recv_task.cancel()
            self._recv_task = None
        if self.zap_socket:
            self._poller.unregister(self.zap_socket)
            self.zap_socket.unbind(self.addr)
            self.zap_socket.close()
        self.zap_socket = None

    async def handle_zap_message(self, msg: List[bytes]):
        """
        修改原始的 handle_zap_message 方法，只是增加地址帧，
        并改为返回 identity，而不是 username
        """
        if len(msg) < 8:
            self.log.error("Invalid ZAP message, not enough frames: %r", msg)
            if len(msg) < 4:
                self.log.error("Not enough information to reply")
            else:
                self._send_zap_reply(msg[1], b"400", b"Not enough frames")
            return

        (
            request_address,
            empty,
            version,
            request_id,
            domain,
            address,
            identity,
            mechanism
        ) = msg[:8]
        if empty:
            raise ValueError
        credentials = msg[8:]

        domain = domain.decode(self.encoding, 'replace')
        address = address.decode(self.encoding, 'replace')

        if version != Settings.ZAP_VERSION:
            self.log.error("Invalid ZAP version: %r", msg)
            await self._asend_zap_reply(
                request_address, request_id,
                b"400", b"Invalid version",
            )
            return

        self.log.debug(
            f"request address: {request_address}, "
            f"version: {version}, request_id: {request_id}, "
            f"domain: {domain}, address: {address}, "
            f"identity: {identity}, mechanism: {mechanism}"
        )

        # Is address is explicitly allowed or _denied?
        allowed = False
        denied = False
        reason = b"NO ACCESS"

        if self._allowed:
            if address in self._allowed:
                allowed = True
                self.log.debug("PASSED (allowed) address=%s", address)
            else:
                denied = True
                reason = b"Address not allowed"
                self.log.debug("DENIED (not allowed) address=%s", address)

        elif self._denied:
            if address in self._denied:
                denied = True
                reason = b"Address denied"
                self.log.debug("DENIED (denied) address=%s", address)
            else:
                allowed = True
                self.log.debug("PASSED (not denied) address=%s", address)

        # Perform authentication mechanism-specific checks if necessary
        username = "anonymous"
        if not denied:
            if mechanism == b'NULL' and not allowed:
                # For NULL, we allow if the address wasn't denied
                self.log.debug("ALLOWED (NULL)")
                allowed = True

            elif mechanism == b'PLAIN':
                # For PLAIN, even a _alloweded address must authenticate
                if len(credentials) != 2:
                    self.log.error("Invalid PLAIN credentials: %r", credentials)
                    await self._asend_zap_reply(
                        request_address, request_id,
                        b"400", b"Invalid credentials",
                        )
                    return
                username, password = (
                    c.decode(self.encoding, 'replace') for c in credentials
                )
                allowed, reason = self._authenticate_plain(
                    domain, username, password
                    )

            elif mechanism == b'CURVE':
                # For CURVE, even a _alloweded address must authenticate
                if len(credentials) != 1:
                    self.log.error("Invalid CURVE credentials: %r", credentials)
                    await self._asend_zap_reply(
                        request_address, request_id,
                        b"400", b"Invalid credentials",
                        )
                    return
                key = credentials[0]
                allowed, reason = await self._authenticate_curve(domain, key)
                if allowed:
                    username = self.curve_user_id(key)

            elif mechanism == b'GSSAPI':
                if len(credentials) != 1:
                    self.log.error(
                        "Invalid GSSAPI credentials: %r", credentials
                        )
                    await self._asend_zap_reply(
                        request_address, request_id,
                        b"400", b"Invalid credentials",
                        )
                    return
                # use principal as user-id for now
                principal = credentials[0]
                username = principal.decode("utf8")
                allowed, reason = self._authenticate_gssapi(domain, principal)

        if allowed:
            await self._asend_zap_reply(
                request_address, request_id,
                b"200", b"OK",
                identity
            )
        else:
            await self._asend_zap_reply(
                request_address, request_id,
                b"400", reason
            )

    async def _asend_zap_reply(
        self, reply_address, request_id,
        status_code, status_text,
        user_id="anonymous"
    ):
        user_id = user_id if status_code == b"200" else b""
        if isinstance(user_id, str):
            user_id = user_id.encode(self.encoding, 'replace')
        metadata = b''  # not currently used
        self.log.debug(
            f"ZAP reply user_id={user_id} request_id={request_id}"
            f" code={status_code} text={status_text}"
        )
        replies = (
            reply_address,
            b"",
            Settings.ZAP_VERSION,
            request_id,
            status_code,
            status_text,
            user_id,
            metadata
        )
        replies = [to_bytes(s) for s in replies]
        await self.zap_socket.send_multipart(replies)


class AMajordomo(_LoopBoundMixin):
    """
    异步管家模式
    Asynchronous Majordomo
    https://zguide.zeromq.org/docs/chapter4/#Asynchronous-Majordomo-Pattern
    多偏好服务集群，连接多个提供不同服务的 Service（远程的 Service 也可能是一个代理，进行负载均衡）
    """

    ctx = Context()

    def __init__(
        self,
        *,
        backend_addr: str = None,
        heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        zap_mechanism: str = None,
        zap_addr: str = None,
        zap_domain: str = Settings.ZAP_DEFAULT_DOMAIN
    ):
        """
        :param backend_addr: 要绑定后端服务的地址，为空则使用默认的本地服务地址。
        :param heartbeat: 需要和后端服务保持的心跳间隔，单位是毫秒。
        :param zap_mechanism: 使用哪一种 zap 验证机制。
        :param zap_addr: zap 身份验证服务的地址。
        :param zap_domain: 需要告诉 zap 服务在哪个域验证。
        """
        # frontend backend
        self.frontend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        self.frontend.set_hwm(Settings.ZMQ_HWM)
        self.frontend_tasks = deque()
        self.backend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        self.backend.set_hwm(Settings.ZMQ_HWM)
        self.backend_tasks = deque()
        if not backend_addr:
            backend_addr = Settings.WORKER_ADDR
        self.backend.bind(check_socket_addr(backend_addr))
        # majordomo
        self.heartbeat = heartbeat
        # 内部程序应该只执行简单的资源消耗少的逻辑处理，并且只能返回状态码
        self.internal_processes: dict[str, Callable] = dict()
        # 不需要用到 ProcessPoolExecutor
        self.internal_process_executor = ThreadPoolExecutor()
        self.services: dict[str, ABCService] = dict()
        self.workers: dict[bytes, RemoteWorker] = dict()
        self.clients: dict[bytes, bytes] = dict()
        self.remote_service_class = Service
        self.remote_worker_class = RemoteWorker
        self.ban_timer_handle: dict[bytes, asyncio.Task] = dict()
        # TODO: 用于跟踪客户端的请求，如果处理请求的后端服务掉线了，向可用的其他后端服务重发。
        self.cache_request = dict()
        self.max_process_tasks = Settings.MESSAGE_MAX
        self.running = False
        self._poller = z_aio.Poller()
        self._broker_task: Optional[asyncio.Task] = None
        # zap
        self.zap_mechanism: str = ""
        self.zap_socket: Union[z_aio.Socket, None] = None
        # TODO: ZAP domain
        self.zap_domain = zap_domain.encode("utf-8")
        self.zap_replies: BoundedDict[str, list] = BoundedDict(
            10*Settings.MESSAGE_MAX, 1.5
        )
        self.zap_tasks = deque()
        if zap_mechanism:
            self.zap_mechanism = zap_mechanism.encode("utf-8")
            if not zap_addr:
                zap_addr = Settings.ZAP_ADDR
            if zap_addr := check_socket_addr(zap_addr):
                self.zap_socket = self.ctx.socket(z_const.DEALER, z_aio.Socket)
                self.zap_socket.set_hwm(Settings.ZMQ_HWM)
                logger.debug(f"Connecting to zap at {zap_addr}")
                self.zap_socket.connect(zap_addr)
                self.zap_request_id = 1
            else:
                raise ValueError(
                    "The ZAP mechanism is specified and "
                    "the ZAP server address must also be provided."
                )
        self.register_internal_process()

    def bind(self, addr: str) -> None:
        """
        绑定到对外地址
        """
        self.frontend.bind(check_socket_addr(addr))

    def close(self):
        self.frontend.close()
        self.backend.close()
        self.internal_process_executor.shutdown()

    def register_internal_process(self):
        """
        注册内部的处理程序
        """
        for name, method in inspect.getmembers(self):
            if (
                    (
                            inspect.ismethod(method)
                            or inspect.iscoroutinefunction(method)
                    )
                    and not name.startswith("_")
                    and getattr(method, "__interface__", False)
            ):
                self.internal_processes[name] = method

    def register_service(self, service: ABCService):
        """
        添加 service
        """
        self.services[service.name] = service

    @interface
    def service(self, **kwargs):
        if from_bytes(kwargs["body"]) in self.services:
            stat_code = b"200"
        else:
            stat_code = b"404"
        return stat_code

    @interface
    def client_heartbeat(self, **kwargs):
        if kwargs["body"] == b"heartbeat":
            stat_code = b"200"
        else:
            stat_code = b"400"
        return stat_code

    def register_worker(self, worker: RemoteWorker):
        self.workers[worker.identity] = worker

    async def ban_worker(self, worker: RemoteWorker, wait: float = 0):
        """
        屏蔽掉指定 worker
        """
        try:
            if wait:
                await asyncio.sleep(wait)
            elif worker.destroy_task:
                worker.destroy_task.cancel()
            if worker.is_alive():
                request = [
                    to_bytes(worker.address),
                    b"",
                    Settings.MDP_WORKER,
                    Settings.MDP_COMMAND_DISCONNECT
                ]
                try:
                    await self.backend.send_multipart(request)
                except Exception:
                    pass
            worker.service.remove_worker(worker)
            if worker.identity in self.workers:
                self.workers.pop(worker.identity, None)
            return True
        except asyncio.CancelledError:
            pass

    def ban_worker_after_expiration(self, worker: RemoteWorker):
        loop = self._get_loop()
        worker.destroy_task = loop.create_task(
            self.ban_worker(worker, wait=worker.expiry)
        )

    async def internal_process(
        self,
        func_name: bytes, client_id: bytes, client_addr: bytes,
        request_id: bytes, body: bytes
    ):
        stat_code = b"501"
        kwargs = {
            "client_id": client_id,
            "client_addr": client_addr,
            "request_id": request_id,
            "body": body
        }
        if func := self.internal_processes.get(from_bytes(func_name), None):
            if inspect.iscoroutinefunction(func):
                stat_code = await func(**kwargs)
            else:
                func = functools.partial(func, **kwargs)
                stat_code = await self._get_loop().run_in_executor(
                    self.internal_process_executor, func
                )
        reply = [
            client_addr,
            b"",
            Settings.MDP_CLIENT,
            Settings.MDP_INTERNAL_SERVICE_PREFIX + func_name,
            client_id,
            request_id,
            stat_code,
        ]
        await self.reply_frontend(reply)

    async def request_zap(
        self, identity, address, request_id, mechanism, credentials
    ) -> bool:
        if mechanism != self.zap_mechanism:
            return False
        zap_frames = [
            b"",  # 空帧
            Settings.ZAP_VERSION,
            request_id,
            self.zap_domain,  # 域
            address,  # IP地址
            identity,  # 身份id
            mechanism,
            *credentials
        ]
        zap_frames = [to_bytes(s) for s in zap_frames]
        reply_key = zap_frames[5] + b"-" + zap_frames[2]
        failed_reply = {
                "user_id": zap_frames[2],
                "request_id": zap_frames[5],
                "metadata": b"",
                "status_code": b"500",
                "status_text": b"ZAP Service Failed.",
            }
        try:
            await self.zap_socket.send_multipart(zap_frames)
        except asyncio.TimeoutError:
            logger.error(f"send ZAP request failed: {zap_frames}.")
            zap_reply = failed_reply
        else:
            try:
                zap_reply = await self.zap_replies.aget(
                    reply_key,
                    timeout=Settings.ZAP_REPLY_TIMEOUT
                )
                await self.zap_replies.apop(
                    reply_key, timeout=Settings.ZAP_REPLY_TIMEOUT,
                    default=None
                )
            except KeyError:
                logger.error(f"get ZAP replies failed: {reply_key}")
                zap_reply = failed_reply
        if zap_reply["status_code"] != b"200":
            logger.error(
                f"Authentication failure: {zap_reply['status_code']}"
                f"({zap_reply['status_text']})"
            )
            return False
        return True

    async def process_replies_from_zap(self, replies):
        """
        当使用 DEALER 类套接字时，需要异步获取验证结果。
        适合使用BoundedDict的超时获取机制。
        """
        try:
            (
                empty,
                zap_version,
                request_id,
                status_code,
                status_text,
                user_id,
                metadata
            ) = replies
        except ValueError:
            logger.debug("ZAP replies frame invalid.")
            return
        await self.zap_replies.aset(
            user_id + b"-" + request_id,
            {
                "user_id": user_id,
                "request_id": request_id,
                "metadata": metadata,
                "status_code": status_code,
                "status_text": status_text,
            }
        )

    def release_frontend_task(self, task: asyncio.Task):
        self.frontend_tasks.remove(task)
        if exception := task.exception():
            exception_info = ''.join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.debug(f"frontend task exception:\n{exception_info}")

    def release_backend_task(self, task: asyncio.Task):
        self.backend_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.debug(f"backend task exception:\n{exception_info}")

    async def process_replies_from_backend(self, replies: list):
        try:
            (
                worker_addr,
                empty,
                mdp_worker_version,
                command,
                worker_id,
                *messages
            ) = replies
            if empty:
                raise ValueError
        except ValueError:
            return
        if mdp_worker_version != Settings.MDP_WORKER:
            return

        if self.zap_socket:
            mechanism, *messages = messages
            if mechanism == Settings.ZAP_MECHANISM_NULL:
                credentials = None
            elif mechanism == Settings.ZAP_MECHANISM_PLAIN:
                credentials = messages[:2]
                messages = messages[2:]
            elif mechanism == Settings.ZAP_MECHANISM_CURVE:
                credentials, *messages = messages
            else:
                logger.debug("ZAP frames invalid")
                return False
        else:
            mechanism = credentials = None

        # MDP 定义的 Worker 命令
        if not (worker := self.workers.get(worker_id, None)):
            logger.debug(f"The worker({worker_id}) was not registered")
        if command == Settings.MDP_COMMAND_READY:
            if len(messages) > 1:
                return
            if mechanism:
                request_id = f"worker_ready{uuid4().hex}"
                zap_result = await self.request_zap(
                    worker_id, worker_addr, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return False

            try:
                service_name, service_description = (
                    from_bytes(messages[0])
                ).split(":")
            except ValueError:
                return False
            if service_name not in self.services:
                service = self.remote_service_class(
                    service_name, service_description
                )
                self.register_service(service)
            else:
                service = self.services[service_name]
            if not worker:
                worker = self.remote_worker_class(
                    worker_id, worker_addr,
                    self.heartbeat, service
                )
            elif worker.ready:
                return await self.ban_worker(worker)
            worker.ready = True
            service.add_worker(worker)
            self.register_worker(worker)
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_HEARTBEAT:
            if len(messages) > 1:
                return
            if mechanism:
                request_id = f"worker_heartbeat{uuid4().hex}"
                zap_result = await self.request_zap(
                    worker_id, worker_addr, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    if worker:
                        await self.ban_worker(worker)
                    return

            if not worker:
                return
            elif not worker.ready:
                return await self.ban_worker(worker)
            worker.expiry = 1e-3 * worker.heartbeat_liveness * worker.heartbeat + time()
            if worker.destroy_task:
                worker.destroy_task.cancel()
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_REPLY:
            try:
                client_id, request_id, *body = messages
            except ValueError:
                return
            if mechanism:
                # 流式回复的request_id是不变的
                zap_result = await self.request_zap(
                    worker_id, worker_addr,
                    request_id + b"-" + uuid4().bytes,
                    mechanism, credentials
                )
                if not zap_result:
                    logger.error(
                        f"ZAP verification of Worker({worker_id}:{worker_addr})"
                        f" fails,'{mechanism}' mechanism is used, and the "
                        f"credential is '{credentials}', and the request_id "
                        f"is '{request_id}'"
                    )
                    return False
            if not (client_addr := self.clients.get(client_id, None)):
                return
            if not worker:
                return
            if not worker.ready:
                return await self.ban_worker(worker)
            service = worker.service
            service.release_worker(worker)
            reply = [
                client_addr,
                empty,
                Settings.MDP_CLIENT,
                worker.service.name,
                client_id,
                request_id,
                *body
            ]
            await self.reply_frontend(reply)

        elif command == Settings.MDP_COMMAND_DISCONNECT:
            if not worker:
                return
            worker.ready = False
            worker.expiry = time()
            await self.ban_worker(worker)

    async def reply_frontend(self, reply: list):
        reply = [to_bytes(frame) for frame in reply]
        try:
            await self.frontend.send_multipart(reply)
        except Exception as error:
            logger.debug(
                f"The reply({reply}) sent to frontend failed.error:\n"
                f"{error}"
            )

    async def process_request_from_frontend(self, request: list):
        try:
            (
                client_addr,
                empty,
                mdp_client_version,
                service_name,
                client_id,
                *messages
            ) = request
            if empty:
                raise ValueError
        except ValueError:
            return False

        self.clients[client_id] = client_addr
        if self.zap_socket:
            try:
                mechanism, *credentials, request_id, body = messages
            except ValueError:
                logger.debug("ZAP frames invalid")
                return False
            zap_result = await self.request_zap(
                client_id, client_addr, request_id, mechanism, credentials
            )
            if not zap_result:
                return False
        else:
            try:
                request_id, body = messages
            except ValueError:
                return False
        if service_name.startswith(Settings.MDP_INTERNAL_SERVICE_PREFIX):
            func_name = service_name.lstrip(Settings.MDP_INTERNAL_SERVICE_PREFIX)
            await self.internal_process(
                func_name, client_id, client_addr,
                request_id, body
            )
        else:
            await self.request_backend(
                service_name,
                client_id, client_addr,
                request_id, body
            )
        return True

    async def request_backend(
        self,
        service_name: bytes, client_id: bytes, client_addr: bytes,
        request_id: bytes, body: bytes
    ):
        service_name = from_bytes(service_name)
        for wid, worker in self.workers.items():
            if not worker.is_alive():
                await self.ban_worker(worker)
        service = self.services.get(service_name)
        exception = await generate_reply(
            exception=ServiceUnAvailableError(service_name)
        )
        except_reply = [
                client_addr,
                b"",
                Settings.MDP_CLIENT,
                service_name,
                client_id,
                request_id,
                msg_pack(exception)
        ]
        if not service or not service.running:
            await self.reply_frontend(except_reply)
        elif worker := service.acquire_idle_worker():
            request = [
                worker.address,
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_REQUEST,
                client_id,
                b"",
                request_id,
                body
            ]
            request = [to_bytes(frame) for frame in request]
            try:
                await self.backend.send_multipart(request)
            except Exception as error:
                await self.reply_frontend(except_reply)
        else:
            exception = await generate_reply(exception=BuysWorkersError())
            except_reply[-1] = msg_pack(exception)
            await self.reply_frontend(except_reply)

    async def send_heartbeat(self):
        for worker in self.workers.values():
            messages = [
                to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_HEARTBEAT
            ]
            await self.backend.send_multipart(messages)

    async def _broker_loop(self):
        loop = self._get_loop()
        self._poller.register(self.frontend, z_const.POLLIN)
        self._poller.register(self.backend, z_const.POLLIN)
        if self.zap_socket:
            self._poller.register(self.zap_socket, z_const.POLLIN)
        self.running = True
        heartbeat_at = 1e-3 * self.heartbeat + loop.time()
        heartbeat_task = loop.create_task(self.send_heartbeat())

        try:
            while 1:
                socks = dict(await self._poller.poll(self.heartbeat))
                if heartbeat_task.done() and loop.time() > heartbeat_at:
                    heartbeat_task = loop.create_task(self.send_heartbeat())
                if self.backend in socks:
                    replies = await self.backend.recv_multipart()
                    bt = loop.create_task(
                        self.process_replies_from_backend(replies),
                        name=f"backend_task_{uuid4().hex}"
                    )
                    self.backend_tasks.append(bt)
                    bt.add_done_callback(self.release_backend_task)

                if self.frontend in socks:
                    if len(self.frontend_tasks) >= self.max_process_tasks:
                        logger.error(
                            f"The number of pending requests from frontend has "
                            f"exceeded the cache limit for the requesting task."
                            f"The limit is {self.max_process_tasks} "
                            f"requests."
                        )
                        await asyncio.sleep(1)
                        continue
                    replies = await self.frontend.recv_multipart()
                    # 同后端任务处理。
                    ft = loop.create_task(
                        self.process_request_from_frontend(replies),
                        name=f"frontend_task_{uuid4().hex}"
                    )
                    self.frontend_tasks.append(ft)
                    ft.add_done_callback(self.release_frontend_task)

                if self.zap_socket and self.zap_socket in socks:
                    replies = await self.zap_socket.recv_multipart()
                    zt = loop.create_task(
                        self.process_replies_from_zap(replies),
                        name=f"zap_task_{uuid4().hex}"
                    )
                    self.zap_tasks.append(zt)
                    zt.add_done_callback(self.zap_tasks.remove)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            logger.error(exception)
            raise
        finally:
            await self.disconnect_all()
            self._poller.unregister(self.frontend)
            self._poller.unregister(self.backend)
            if self.zap_socket:
                self._poller.unregister(self.zap_socket)
            logger.info("broker loop exit.")

    async def disconnect_all(self):
        # TODO: 需要通知客户端吗？
        logger.warning("Disconnected and all the workers.")
        for work in list(self.workers.values()):
            await self.ban_worker(work)

    def run(self):
        if not self._broker_task:
            loop = self._get_loop()
            self._broker_task = loop.create_task(self._broker_loop())

    def stop(self):
        if self._broker_task:
            if not self._broker_task.cancelled():
                self._broker_task.cancel()
            self.close()
            self._broker_task = None


class Client(_LoopBoundMixin):

    def __init__(
        self,
        broker_addr: str,
        identity: str = None,
        context: Context = None,
        heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        reply_timeout: float = Settings.REPLY_TIMEOUT,
        msg_max: int = Settings.MESSAGE_MAX,
        zap_mechanism: str = None,
        zap_credentials: tuple = None,
    ):
        """
        客户端，
        TODO：连接远程的 MDP 服务，连接后发送一条查询有哪些服务的消息，
          然后将包含的服务添加为客户端关联的服务实例，链式调用服务包含的方法。
        TODO: 对于没有收到回复的请求记录并保存下来，可以设置重试次数来重新请求。
        :param context: zmq 的上下文实例，为空则创建。
        :param heartbeat: 维持连接的心跳消息间隔，默认为 MDP_HEARTBEAT，单位是毫秒。
        :param reply_timeout: 等待回复的超时时间
        :param msg_max: 保存发送或接收到的消息的最大数量，默认为 MSSAGE_MAX。
        :param zap_mechanism: zap 的验证机制。
        :param zap_credentials: zap 的凭据帧列表，不填则为空列表。
        """
        if not identity:
            identity = uuid4().hex
        self.identity = f"gc-{identity}".encode("utf-8")
        self.ready = False
        self._broker_addr = check_socket_addr(broker_addr)
        zap_mechanism, zap_credentials = check_zap_args(
            zap_mechanism, zap_credentials
        )
        if not zap_mechanism:
            self.zap_frames = []
        else:
            self.zap_frames = [zap_mechanism, *zap_credentials]

        if not context:
            self.ctx = Context()
        else:
            self.ctx = context
        self.socket = None
        self._poller = z_aio.Poller()

        self.heartbeat = heartbeat
        self.heartbeat_expiry = heartbeat * Settings.MDP_HEARTBEAT_LIVENESS
        self.reply_timeout = reply_timeout

        self.requests: BoundedDict[str, list] = BoundedDict(msg_max, 1.5)
        self.stream_reply_maxsize = Settings.STREAM_REPLY_MAXSIZE
        self._recv_task = None

        self.default_service = Settings.SERVICE_DEFAULT_NAME
        self._remote_service = self._remote_func = None

        self.connect()

    def connect(self):
        if not self.ready:
            loop = self._get_loop()
            logger.info(f"Connecting to broker at {self._broker_addr} ...")
            self.socket = self.ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
            self.socket.set_hwm(Settings.ZMQ_HWM)
            self.socket.connect(self._broker_addr)
            self._poller.register(self.socket, z_const.POLLIN)
            self.ready = True
            self._recv_task = loop.create_task(self._recv())

    def disconnect(self):
        if self.ready:
            self._poller.unregister(self.socket)
            self.socket.disconnect(self._broker_addr)
            self.socket.close()
            self.socket = None

    def close(self):
        if self.ready:
            self.disconnect()
            if not self._recv_task.cancelled:
                self._recv_task.cancel()
            self._recv_task = None
            self.ready = False

    def _clear_reply_later(self, request_id, delay):
        if request_id in self.requests:
            if isinstance(reply := self.requests[request_id], StreamReply):
                if reply._exit:
                    self.requests.pop(request_id)
                else:
                    self._get_loop().call_later(
                        delay,
                        self._clear_reply_later,
                        request_id, delay
                    )
            else:
                self.requests.pop(request_id)

    async def _request(self, service_name: str, body: Union[str, bytes]) -> bytes:
        """
        客户端使用 DEALER 类型 socket，每个请求都有一个id，
        worker 每个回复都会带上这个请求id。 客户端需要将回复和请求对应上。
        :param service_name: 要访问的服务的名字
        """
        request_id = uuid4().hex
        request = [
            b"",
            Settings.MDP_CLIENT,
            service_name,
            self.identity,
            *self.zap_frames,
            request_id,
            body
        ]
        request = [to_bytes(frame) for frame in request]
        await self.socket.send_multipart(request)
        return request[-2]

    async def knock(
        self, service_name: str, func_name: str, *args, **kwargs
    ):
        """
        调用指定的远程服务的指定方法。获取返回结果，如果有异常就重新抛出异常，
        :param service_name: 要访问的服务的名字
        :param func_name: 要调用该服务的函数的名字
        :param args: 要调用的该函数要传递的位置参数
        :param kwargs: 要调用的该函数要传递的关键字参数
        """
        if self.requests.full():
            raise RuntimeError(
                "The number of pending requests has exceeded the cache limit"
                " for the requesting task. The limit is "
                f"{self.requests.maxsize} requests."
            )
        loop = self._get_loop()
        body = msg_pack(
            {
                "func": func_name,
                "args": args,
                "kwargs": kwargs
            }
        )
        request_id = await self._request(service_name, body)
        try:
            response = await self.requests.aget(
                request_id, timeout=self.reply_timeout
            )
            if isinstance(response, StreamReply):
                return response
            elif isinstance(response, HugeData):
                result = b""
                throw = False
                async for data in response.decompress(
                        Settings.HUGE_DATA_SIZEOF
                        ):
                    if throw:
                        exception = "\n".join(data)
                        raise HugeDataException(exception)
                    if data == HugeData.except_tag:
                        throw = True
                    else:
                        result += data
                with ProcessPoolExecutor() as executor:
                    _msg_unpack = functools.partial(
                        msg_unpack, result
                    )
                    result = await loop.run_in_executor(executor, _msg_unpack)
                return result
            else:
                if exception := response.get("exception"):
                    raise RemoteException(*exception)
                return response.get("result")
        except KeyError:
            raise TimeoutError(
                "Timeout while requesting the Majordomo service."
            )
        finally:
            loop.call_later(
                1,
                self._clear_reply_later,
                request_id, 1
            )

    async def _majordomo_service(self, service_name: bytes, body: bytes):
        service_name = Settings.MDP_INTERNAL_SERVICE_PREFIX + service_name
        return await self._request(service_name, body)

    async def _send_heartbeat(self):
        body = b"heartbeat"
        return await self._majordomo_service(
            b"client_heartbeat", body
        )

    def _query_service(self, service_name: bytes):
        loop = self._get_loop()
        return loop.run_until_complete(
            self._majordomo_service(b"service", service_name)
        )

    async def _recv(self):
        loop = self._get_loop()
        heartbeat_at = 1e-3 * self.heartbeat + loop.time()
        try:
            while 1:
                if not self.ready:
                    break
                if loop.time() > heartbeat_at:
                    await self._send_heartbeat()
                    heartbeat_at = 1e-3 * self.heartbeat + loop.time()
                socks = await self._poller.poll(self.heartbeat)
                if not socks:
                    continue
                messages = await self.socket.recv_multipart()
                try:
                    (
                        empty,
                        mdp_client_version,
                        service_name,
                        client_id,
                        request_id,
                        # steam tag
                        *body
                    ) = messages
                    if empty:
                        continue
                except ValueError:
                    continue
                if mdp_client_version != Settings.MDP_CLIENT:
                    continue
                if client_id != self.identity:
                    continue
                if service_name == b"gate.client_heartbeat":
                    continue
                logger.debug(
                    f"Reply: \n"
                    f"frame 0: {empty},\n"
                    f"frame 1: {mdp_client_version},\n"
                    f"frame 2: {service_name},\n"
                    f"frame 3: {client_id},\n"
                    f"frame 4: {request_id},\n"
                    f"frame 5+: {body}"
                )
                if len(body) == 1:
                    body = msg_unpack(body[0])
                    await self.requests.aset(request_id, body)
                else:
                    *option, body = body
                    if option[0] == Settings.STREAM_GENERATOR_TAG:
                        stream_reply = self.requests.get(request_id, None)
                        if not stream_reply:
                            stream_reply = StreamReply(
                                maxsize=self.stream_reply_maxsize,
                                timeout=self.reply_timeout
                            )
                            await self.requests.aset(
                                request_id, stream_reply
                            )
                        if body != Settings.STREAM_END_MESSAGE:
                            body = msg_unpack(body)
                        await stream_reply.asend(body)
                    elif option[0] == Settings.STREAM_HUGE_DATA_TAG:
                        huge_reply = self.requests.get(request_id, None)
                        if not huge_reply:
                            huge_reply = HugeData()
                            await self.requests.aset(
                                request_id, huge_reply
                            )
                        huge_reply.data.put_nowait(body)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            logger.error(exception)
            raise

    def __getattr__(self, item):
        if not self._remote_func:
            self._remote_service = self.default_service
        else:
            if self._remote_service == self.default_service:
                self._remote_service = self._remote_func
            else:
                self._remote_service = ".".join(
                    (self._remote_service, self._remote_func)
                )
        self._remote_func = item
        return self

    def __call__(self, *args, **kwargs) -> Coroutine:
        """
        返回一个执行rpc调用的协程
        """
        waiting = self.knock(
                self._remote_service, self._remote_func, *args, **kwargs
            )
        self._remote_service = self._remote_func = None
        return waiting
