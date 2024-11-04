# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/3/30 11:09
import asyncio
import inspect
import secrets
import socket
import struct
import sys
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Coroutine, Callable, AsyncGenerator, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from traceback import format_exception, format_tb
from typing import Annotated, Any, List, Tuple, Union, overload, Optional
from logging import getLogger
from uuid import uuid4

import zmq.constants as z_const
import zmq.asyncio as z_aio
import zmq.error as z_error
from zmq.backend import curve_keypair
from zmq import Context as SyncContext
from zmq import Socket as SyncSocket
from zmq import ZMQError
from zmq.auth import Authenticator

from .global_settings import Settings
from .exceptions import (
    BusyGateError, BusyWorkerError,
    GateUnAvailableError, ServiceUnAvailableError,
    RemoteException,
)
from .utils import (
    MulticastProtocol, HugeData, LRUCache, MsgUnpackError, StreamReply,
    check_socket_addr, Empty, empty, generator_to_agenerator,
    throw_exception_agenerator,
    interface, run_in_executor, from_bytes, msg_pack, msg_unpack, to_bytes,
    name2uuid
)


__all__ = [
    "AsyncZAPService", "Context",
    "Worker", "RemoteWorker", "Service", "AMajordomo", "Client",
    "Gate", "RemoteGate", "GateClusterService",

]

logger = getLogger("gaterpc")
Wlogger = getLogger("gaterpc.worker")
Mlogger = getLogger("gaterpc.majordomo")
Glogger = getLogger("gaterpc.gate")
Clogger = getLogger("gaterpc.client")


async def generate_reply(
    result: Any = None, exception: Exception = None
):
    if exception:
        exception = (
            exception.__class__.__name__,
            exception.__repr__(),
            "".join(format_tb(exception.__traceback__))
        )
    if isinstance(result, Generator):
        result = generator_to_agenerator(result)
    if isinstance(result, (AsyncGenerator, HugeData)):
        return result
    else:
        return {
            "result": result,
            "exception": exception
        }


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


def set_ctx(ctx: Context, options: dict = None):
    _options = Settings.ZMQ_CONTEXT
    if options:
        _options.update(options)
    for op, v in _options.items():
        try:
            logger.debug(f"ctx({ctx}).{op.name} is {ctx.get(op)}:")
            ctx.set(op, v)
            logger.debug(f"ctx({ctx}) update {op.name} = {v}")
        except AttributeError as e:
            logger.error(e)


def set_sock(sock: z_aio.Socket, options: dict = None):
    _options = Settings.ZMQ_SOCK
    if options:
        _options.update(options)
    for op, v in _options.items():
        try:
            if op == z_const.HWM and isinstance(v, int):
                logger.debug(f"sock({sock}).{op.name} is {sock.get_hwm()}")
                sock.set_hwm(v)
            else:
                try:
                    logger.debug(f"sock({sock}).{op.name} is {sock.get(op)}")
                except ZMQError:
                    logger.debug(f"sock({sock}).{op.name} is not set.")
                sock.set(op, v)
            logger.debug(f"sock({sock}) update {op.name} = {v}")
        except AttributeError as e:
            logger.error(e)


class ABCWorker(ABC):
    """
    Worker 的抽象基类，可以根据资源需求设置是否限制接收的请求数量。
    :param identity: 身体id
    :param service: 用于表示功能的服务
    :param heartbeat_liveness: 心跳活跃度，允许丢失多少次心跳
    :param heartbeat: 心跳间隔，单位是毫秒
    """
    identity: bytes
    service: "ABCService"
    heartbeat_liveness: int
    heartbeat: int
    expiry: float
    max_allowed_request: int

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
    workers: dict[bytes, ABCWorker]
    idle_workers: deque[bytes]

    @abstractmethod
    def add_worker(self, worker: Optional[ABCWorker]) -> None:
        pass

    @abstractmethod
    def remove_worker(self, worker: Optional[ABCWorker]) -> None:
        pass

    @abstractmethod
    def get_workers(self) -> dict[bytes, ABCWorker]:
        pass

    @abstractmethod
    async def acquire_idle_worker(self) -> ABCWorker:
        pass

    @abstractmethod
    async def release_worker(self, worker: Optional[ABCWorker]):
        pass


class RemoteWorker(ABCWorker):

    def __init__(
        self,
        identity: bytes,
        heartbeat: int,
        max_allowed_request: int,
        sock: z_aio.Socket,
        mdp_version: bytes,
        service: "ABCService"
    ) -> None:
        """
        远程 worker 的本地映射
        :param identity: 远程 worker 的id
        :param heartbeat: 和远程 worker 的心跳间隔，单位是毫秒
        :param max_allowed_request: 能处理的最大请求数
        :param sock: sock
        :param mdp_version: MDP版本
        :param service: 该 worker 属于哪个服务
        """
        self.identity = identity
        self.sock = sock
        self.mdp_version = mdp_version
        self.ready = True
        self.service = service
        if heartbeat:
            self.heartbeat = heartbeat
        else:
            self.heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS
        self.last_message_sent_time: float = 0.0
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.max_allowed_request = max_allowed_request
        self.destroy_task: Optional[asyncio.Task] = None

    def is_alive(self):
        # TODO: 如果在心跳间隔时间内下线，该如何做？
        return self.ready

    def prolong(self, now_time):
        self.expiry = (
                1e-3 * self.heartbeat * self.heartbeat_liveness + now_time
        )


class Worker(ABCWorker):
    """
    所有要给远端调用的方法都需要用interface函数装饰。
    """

    def __init__(
        self,
        broker_addr: str,
        service: "Service",
        identity: str = None,
        heartbeat: int = None,
        context: Context = None,
        zap_mechanism: Union[str, bytes] = None,
        zap_credentials: tuple = None,
        max_allowed_request: int = None,
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
        :param max_allowed_request: 能处理的最大请求数
        :param thread_executor: 用于执行io密集型任务
        :param process_executor: 用于执行cpu密集型任务
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        if not identity:
            identity = f"gw{secrets.token_hex(8)}"
        self.identity = name2uuid(identity).bytes
        if Settings.DEBUG > 1:
            Wlogger.debug(f"Worker id: {self.identity}")
        self.service = service
        self.ready = False
        self._broker_addr = check_socket_addr(broker_addr)
        zap_mechanism, zap_credentials = check_zap_args(
            zap_mechanism, zap_credentials
        )
        if zap_mechanism:
            self.zap_frames = (zap_mechanism, *zap_credentials)
        else:
            self.zap_frames = None

        if not context:
            self._ctx = Context()
        else:
            self._ctx = context
        set_ctx(self._ctx)
        self._socket = None
        self._poller = z_aio.Poller()
        if not heartbeat:
            heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat = heartbeat
        self.heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS

        if max_allowed_request:
            self.max_allowed_request = max_allowed_request
        else:
            self.max_allowed_request = Settings.MESSAGE_MAX
        self.requests = set()
        self.sent_tasks = set()

        if not thread_executor:
            self.thread_executor = ThreadPoolExecutor()
        else:
            self.thread_executor = thread_executor
        if not process_executor:
            self.process_executor = ProcessPoolExecutor()
        else:
            self.process_executor = process_executor

        self.interfaces: dict = self._get_interfaces()

        self._reconnect: bool = False
        self._recv_task: Optional[asyncio.Task] = None

    def _get_interfaces(self):
        interfaces = dict()
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
                    "signature": str(inspect.signature(method))
                }
        return interfaces

    @interface
    def get_interfaces(self):
        return self.interfaces

    def is_alive(self):
        return self.ready

    async def connect(self):
        if not self.is_alive():
            Wlogger.info(
                f"Worker is attempting to connect to the broker at "
                f"{self._broker_addr}."
            )
            self._socket = self._ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
            set_sock(self._socket, {z_const.IDENTITY: self.identity})
            self._socket.connect(self._broker_addr)
            self._poller.register(self._socket, z_const.POLLIN)
            service = Settings.MDP_DESCRIPTION_SEP.join((
                self.service.name, self.service.description,
                str(self.max_allowed_request)
            ))
            await self.send_to_majordomo(
                Settings.MDP_COMMAND_READY,
                (service,)
            )
            self.ready = True

    async def disconnect(self):
        if self.is_alive():
            Wlogger.warning("Disconnecting")
            self._poller.unregister(self._socket)
            self._socket.disconnect(self._broker_addr)
            self._socket.close()
            self._socket = None
            self.ready = False

    def close(self):
        """
        关闭打开的资源。
        """
        self.thread_executor.shutdown()
        self.process_executor.shutdown()

    def run(self):
        if not self._recv_task:
            self._recv_task = self._loop.create_task(self.recv())

    def stop(self):
        if self._recv_task and not self._recv_task.cancelled():
            self._recv_task.cancel()
            self._recv_task = None

    def cleanup_sent(self, task: asyncio.Task):
        self.sent_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Wlogger.error(f"sent task exception:\n{exception_info}")

    def cleanup_request(self, task: asyncio.Task):
        self.requests.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Wlogger.error(f"process request task exception:\n{exception_info}")

    async def build_zap_frames(self) -> tuple[bytes, ...]:
        """
        用于继承后根据 ZAP 校验方法构建 ZAP 凭据帧，
        使用协程是为了方便使用 loop.run_in_executor
        """
        if self.zap_frames:
            return tuple(to_bytes(z) for z in self.zap_frames)
        else:
            return tuple()

    async def send_to_majordomo(
        self,
        command: bytes,
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[Union[bytes, str], tuple[Union[bytes, str], ...]] = None
    ):
        if not self._socket:
            return
        if isinstance(messages, (bytes, str)):
            messages = (messages,)
        elif not messages:
            messages = tuple()
        if option:
            messages = (*option, *messages)
        messages = (
            b"",
            Settings.MDP_WORKER,
            command,
            *(await self.build_zap_frames()),
            *messages
        )
        messages = tuple(to_bytes(s) for s in messages)
        await self._socket.send_multipart(messages)

    async def send_hugedata(self, option: tuple, hugedata):
        try:
            async for data in hugedata.compress(self._loop):
                await self.send_to_majordomo(
                    Settings.MDP_COMMAND_REPLY,
                    option,
                    (Settings.STREAM_HUGE_DATA_TAG, data)
                )
            await self.send_to_majordomo(
                Settings.MDP_COMMAND_REPLY,
                option,
                (Settings.STREAM_HUGE_DATA_TAG, hugedata.end_tag)
            )
        except Exception as e:
            e = (
                e.__class__.__name__,
                e.__repr__(),
                "".join(format_tb(e.__traceback__))
            )
            Wlogger.error(e)
            e = msg_pack(e)
            await self.send_to_majordomo(
                Settings.MDP_COMMAND_REPLY,
                option,
                (
                    Settings.STREAM_HUGE_DATA_TAG,
                    Settings.STREAM_EXCEPT_TAG,
                    e
                )
            )
        finally:
            hugedata.destroy()

    async def send_agenerator(self, option: tuple, agenerator: AsyncGenerator):
        try:
            async for sub_reply in agenerator:
                messages = msg_pack(sub_reply)
                await self.send_to_majordomo(
                    Settings.MDP_COMMAND_REPLY, option,
                    (Settings.STREAM_GENERATOR_TAG, messages)
                )
            await self.send_to_majordomo(
                Settings.MDP_COMMAND_REPLY, option,
                (
                    Settings.STREAM_GENERATOR_TAG, Settings.STREAM_END_TAG
                )
            )
        except Exception as e:
            e = (
                e.__class__.__name__,
                e.__repr__(),
                "".join(format_tb(e.__traceback__))
            )
            e = msg_pack(e)
            Wlogger.error(e)
            await self.send_to_majordomo(
                Settings.MDP_COMMAND_REPLY, option,
                (
                    Settings.STREAM_GENERATOR_TAG,
                    Settings.STREAM_EXCEPT_TAG,
                    e
                )
            )

    async def process_request(self, messages):
        try:
            (
                client_id,
                empty_frame,
                request_id,
                *messages,
            ) = messages
            if empty_frame:
                return
            if messages is empty:
                return
            (func_name, *params) = messages
            if not params:
                args, kwargs = tuple(), dict()
            elif len(params) == 1:
                args, kwargs = msg_unpack(params[0]), dict()
            elif len(params) == 2:
                args, kwargs = (msg_unpack(p) for p in params)
            else:
                return
            func_name = msg_unpack(func_name)
        except (MsgUnpackError, ValueError):
            return
        result = exception = None
        try:
            if len(self.requests) > self.max_allowed_request:
                if not Settings.SECURE:
                    return
                raise RuntimeError(
                    f"Request count exceeds the maximum value("
                    f"{self.max_allowed_request})"
                )
            if not self.interfaces.get(func_name, None):
                if Settings.SECURE:
                    return
                raise AttributeError(
                    "Function not found: {0}".format(func_name)
                )
            func = getattr(self, func_name)
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                if func.__executor__ == "thread":
                    result = await run_in_executor(
                                self._loop, self.thread_executor,
                                func, *args, **kwargs
                            )
                elif func.__executor__ == "process":
                    result = await run_in_executor(
                                self._loop, self.process_executor,
                                func, *args, **kwargs
                            )
                else:
                    result = func(*args, **kwargs)
        except Exception as e:
            exception = e
        option = (client_id, b"", request_id)
        if isinstance(result, HugeData):
            await self.send_hugedata(option, result),
        elif isinstance(result, AsyncGenerator):
            await self.send_agenerator(option, result),
        else:
            reply = msg_pack(
                await generate_reply(result=result, exception=exception)
            )
            await self.send_to_majordomo(
                    Settings.MDP_COMMAND_REPLY, option, reply
                )

    async def recv(self):
        try:
            await self.connect()
            Wlogger.info("Worker event loop starts running.")
            majordomo_liveness = self.heartbeat_liveness
            while 1:
                if not self.ready:
                    break
                socks = dict(await self._poller.poll(self.heartbeat))
                if not socks:
                    majordomo_liveness -= 1
                    if majordomo_liveness == 0:
                        Wlogger.error("Majordomo offline")
                        break
                    continue
                messages = await self._socket.recv_multipart()
                try:
                    (
                        empty_frame,
                        mdp_worker_version,
                        command,
                        *messages
                    ) = messages
                    if empty_frame:
                        continue
                except ValueError:
                    continue
                if mdp_worker_version != Settings.MDP_WORKER:
                    continue
                majordomo_liveness = self.heartbeat_liveness
                if Settings.DEBUG > 1:
                    Wlogger.debug(
                        f"worker recv\n"
                        f"command: {command}\n"
                        f"messages: {messages}"
                    )
                if command == Settings.MDP_COMMAND_HEARTBEAT:
                    self.sent_tasks.add(
                        task := self._loop.create_task(
                            self.send_to_majordomo(
                                Settings.MDP_COMMAND_HEARTBEAT
                            )
                        )
                    )
                    task.add_done_callback(self.cleanup_sent)
                    continue
                elif command == Settings.MDP_COMMAND_DISCONNECT:
                    Wlogger.warning(f"{self.identity} disconnected")
                    break
                elif command == Settings.MDP_COMMAND_REQUEST:
                    self.requests.add(
                        task := self._loop.create_task(
                            self.process_request(messages)
                        )
                    )
                    task.add_done_callback(self.cleanup_request)
        except asyncio.CancelledError:
            self._reconnect = False
            pass
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Wlogger.error(exception)
            raise
        finally:
            await self.disconnect()
            if self._loop.is_running() and self._reconnect:
                Wlogger.warning("Reconnect to Majordomo")
                self._recv_task = self._loop.create_task(self.recv())
            else:
                self.close()


class Service(ABCService):
    """
    管理本地和远程提供该服务的 worker。
    TODO: 如何避免有同名但接口不一样的服务连上来，会导致客户端调用本该有却没有的接口而出错。
    """

    def __init__(
        self, name: str = None,
        description: str = "gate-rpc service"
    ):
        """
        :param name: 该服务的名称
        :param description: 描述该服务能提供的功能
        """
        if not name:
            name = Settings.SERVICE_DEFAULT_NAME
        self.name = name
        self.description: Annotated[str, "Keep it short"] = description
        self.workers: dict[bytes, Union[Worker, RemoteWorker]] = dict()
        self.idle_workers: asyncio.Queue[bytes] = asyncio.Queue()
        self.running = False

    def add_worker(self, worker: Union[Worker, RemoteWorker]):
        if worker.identity in self.workers:
            return
        worker.service = self
        self.workers[worker.identity] = worker
        self.idle_workers.put_nowait(worker.identity)
        self.running = True

    def remove_worker(self, worker: Union[Worker, RemoteWorker]):
        if worker.identity in self.workers:
            self.workers.pop(worker.identity)
        if hasattr(worker, "stop"):
            worker.stop()
        if not self.workers:
            self.running = False

    def get_workers(self) -> dict[bytes, Union[Worker, RemoteWorker]]:
        return self.workers

    async def acquire_idle_worker(self) -> Union[Worker, RemoteWorker]:
        if not self.running:
            raise ServiceUnAvailableError(self.name)
        while 1:
            try:
                worker_id = await asyncio.wait_for(
                    self.idle_workers.get(),
                    Settings.TIMEOUT
                )
            except asyncio.TimeoutError:
                raise BusyWorkerError()
            try:
                worker = self.workers[worker_id]
                if worker.is_alive() and worker.max_allowed_request != 0:
                    worker.max_allowed_request -= 1
                    if worker.max_allowed_request > 0:
                        await self.idle_workers.put(worker.identity)
                    return worker
            except KeyError:
                continue

    async def release_worker(self, worker: Union[Worker, RemoteWorker]):
        if worker.identity in self.workers and worker.is_alive():
            if worker.max_allowed_request >= 0:
                await self.idle_workers.put(worker.identity)
            worker.max_allowed_request += 1

    def create_worker(
        self,
        worker_class: Callable[..., Worker] = None,
        workers_addr: str = None,
        workers_heartbeat: int = None,
        context: Context = None,
        zap_mechanism: str = None,
        zap_credentials: tuple = None,
        max_allowed_request: int = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ) -> Worker:
        if not workers_addr:
            workers_addr = Settings.WORKER_ADDR
        if not workers_heartbeat:
            workers_heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        if not worker_class:
            worker_class = Worker
        worker = worker_class(
            workers_addr,
            self,
            identity=f"{self.name}Worker-{secrets.token_hex(8)}",
            heartbeat=workers_heartbeat,
            context=context,
            zap_mechanism=zap_mechanism,
            zap_credentials=zap_credentials,
            max_allowed_request=max_allowed_request,
            thread_executor=thread_executor,
            process_executor=process_executor
        )
        self.add_worker(worker)
        return worker


class AsyncZAPService(Authenticator):
    """
    异步的 zap 身份验证服务，继承并重写为使用 ROUTE 模式
    """
    context: Context
    zap_socket: Optional[z_aio.Socket]
    encoding = "utf-8"

    def __init__(self, addr: str = None, context=None):
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        if not context:
            context = Context()
        set_ctx(context)
        super().__init__(context=context, log=getLogger("gaterpc.zap"))
        if not addr:
            addr = Settings.ZAP_ADDR
        self.addr = addr
        self.zap_socket = self.context.socket(z_const.ROUTER, z_aio.Socket)
        set_sock(self.zap_socket)
        self.zap_socket.bind(check_socket_addr(addr))
        self._poller = z_aio.Poller()
        self._recv_task: Optional[asyncio.Task] = None
        self.request = set()

    async def handle_zap(self) -> None:
        self._poller.register(self.zap_socket, z_const.POLLIN)
        self.log.debug("ZAP Service start.")
        try:
            while 1:
                socks = dict(await self._poller.poll())
                if self.zap_socket in socks:
                    message = await self.zap_socket.recv_multipart()
                    t = self._loop.create_task(self.handle_zap_message(message))
                    self.request.add(t)
                    t.add_done_callback(self.cleanup_request)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            self.log.warning(exception)
            self.stop()
            raise

    def start(self) -> None:
        if not self._recv_task:
            self.log.debug(f"create ZAP task.")
            self._recv_task = self._loop.create_task(self.handle_zap())

    def stop(self) -> None:
        if self._recv_task and not self._recv_task.cancelled():
            self._recv_task.cancel()
            self._recv_task = None
        if self.zap_socket:
            self._poller.unregister(self.zap_socket)
            self.zap_socket.unbind(self.addr)
            self.zap_socket.close()
        self.zap_socket = None

    def cleanup_request(self, task: asyncio.Task):
        self.request.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__cause__}:{exception.__repr__()}\n"
                              f"{exception_info}")
            logger.error(f"process request task exception:\n{exception_info}")

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
            empty_frame,
            version,
            request_id,
            domain,
            address,
            identity,
            mechanism
        ) = msg[:8]
        if empty_frame:
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
        if Settings.DEBUG > 1:
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
            else:
                denied = True
                reason = b"Address not allowed"

        elif self._denied:
            if address in self._denied:
                denied = True
                reason = b"Address denied"
            else:
                allowed = True

        # Perform authentication mechanism-specific checks if necessary
        username = "anonymous"
        if not denied:
            if mechanism == b'NULL' and not allowed:
                # For NULL, we allow if the address wasn't denied
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
                username
            )
        else:
            await self._asend_zap_reply(
                request_address, request_id,
                b"400", reason
            )

    def _authenticate_plain(
        self, domain: str, username: str, password: str
    ) -> Tuple[bool, bytes]:
        allowed = False
        reason = b""
        if self.passwords:
            # If no domain is not specified then use the default domain
            if not domain:
                domain = '*'

            if domain in self.passwords:
                if username in self.passwords[domain]:
                    if password == self.passwords[domain][username]:
                        allowed = True
                    else:
                        reason = b"Invalid password"
                else:
                    reason = b"Invalid username"
            else:
                reason = b"Invalid domain"
            if Settings.DEBUG > 1:
                if allowed:
                    self.log.debug(
                        "ALLOWED (PLAIN) domain=%s username=%s password=%s",
                        domain,
                        username,
                        password,
                    )
                else:
                    self.log.debug("DENIED %s", reason)

        else:
            reason = b"No passwords defined"

        return allowed, reason

    async def _asend_zap_reply(
        self, reply_address, request_id,
        status_code, status_text,
        user_id="anonymous"
    ):
        user_id = user_id if status_code == b"200" else b""
        if isinstance(user_id, str):
            user_id = user_id.encode(self.encoding, 'replace')
        metadata = b''  # not currently used
        if Settings.DEBUG > 1:
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
        replies = tuple(to_bytes(s) for s in replies)
        await self.zap_socket.send_multipart(replies)


def check_zap_args(
    mechanism: Union[str, bytes, None], credentials: Optional[tuple]
) -> tuple[Union[bytes, None], Optional[tuple]]:
    if not mechanism:
        return mechanism, credentials
    elif isinstance(mechanism, str):
        mechanism = mechanism.encode("utf-8")
    if credentials is None:
        credentials = tuple()
    if (mechanism == Settings.ZAP_MECHANISM_NULL
            and len(credentials) != 0):
        AttributeError(
            f'The "{Settings.ZAP_MECHANISM_NULL}" mechanism '
            f'should not have credential frames.'
        )
    elif (mechanism == Settings.ZAP_MECHANISM_PLAIN
          and len(credentials) != 2):
        AttributeError(
            f'The "{Settings.ZAP_MECHANISM_PLAIN}" mechanism '
            f'should have tow credential frames: '
            f'a username and a password.'
        )
    elif mechanism == Settings.ZAP_MECHANISM_CURVE:
        raise RuntimeError(
            f'The "{Settings.ZAP_MECHANISM_CURVE}"'
            f' mechanism is not implemented yet.'
        )
    elif mechanism not in (
            Settings.ZAP_MECHANISM_NULL,
            Settings.ZAP_MECHANISM_PLAIN,
            Settings.ZAP_MECHANISM_CURVE
    ):
        raise ValueError(
            f'mechanism can only be '
            f'"{Settings.ZAP_MECHANISM_NULL}" or '
            f'"{Settings.ZAP_MECHANISM_PLAIN}" or '
            f'"{Settings.ZAP_MECHANISM_CURVE}"'
        )
    return mechanism, credentials


def parse_zap_frame(messages):
    mechanism, *messages = messages
    if mechanism == Settings.ZAP_MECHANISM_NULL:
        return mechanism, None, tuple(messages)
    elif mechanism == Settings.ZAP_MECHANISM_PLAIN:
        return mechanism, tuple(messages[:2]), tuple(messages[2:])
    elif mechanism == Settings.ZAP_MECHANISM_CURVE:
        credentials, *messages = messages
        return mechanism, credentials, tuple(messages)
    else:
        return None


class AMajordomo:
    """
    异步管家模式
    Asynchronous Majordomo
    https://zguide.zeromq.org/docs/chapter4/#Asynchronous-Majordomo-Pattern
    多偏好服务集群，连接多个提供不同服务的 Service（远程的 Service 也可能是一个代理，进行负载均衡）
    """

    RemoteServiceClass = Service
    RemoteWorkerClass = RemoteWorker

    def __init__(
        self,
        *,
        identity: str = None,
        context: Context = None,
        heartbeat: int = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ):
        """
        :param identity: 用于识别的id
        :param context: 在使用 inproc 时使用同一上下文
        :param heartbeat: 需要和后端服务保持的心跳间隔，单位是毫秒。
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        if not identity:
            identity = f"gm-{secrets.token_hex(8)}"
        self.identity = name2uuid(identity).bytes
        if Settings.DEBUG > 1:
            Mlogger.debug(f"AMajordomo id: {self.identity}")
        if not context:
            context = Context()
        self.ctx = context
        set_ctx(self.ctx)
        # frontend
        self.frontend: Optional[z_aio.Socket] = None
        self.clients: set[bytes] = set()
        self.frontend_tasks: set[asyncio.Task] = set()
        # backend
        self.backend: Optional[z_aio.Socket] = None
        self.services: dict[str, Service] = dict()
        self.workers: dict[bytes, RemoteWorker] = dict()
        self.backend_tasks: set[asyncio.Task] = set()
        # zap
        self.zap: Optional[z_aio.Socket] = None
        self.zap_addr: str = ""
        self.zap_domain: bytes = b""
        self.zap_replies: dict[bytes, asyncio.Future] = dict()
        self.zap_cache = LRUCache(128)
        self.zap_tasks: set[asyncio.Task] = set()
        # majordomo
        if not heartbeat:
            heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat = heartbeat
        self.internal_func: dict[str, dict] = dict()
        if not thread_executor:
            self.thread_executor = ThreadPoolExecutor()
        else:
            self.thread_executor = thread_executor
        if not process_executor:
            self.process_executor = ProcessPoolExecutor()
        else:
            self.process_executor = process_executor
        # 执行内部方法的任务，或者内部子任务
        self.internal_tasks: dict[
            asyncio.Task,
            tuple[Callable[..., Coroutine], str, bool, tuple, dict]
        ] = dict()
        # 保存由内部向后端发起的请求
        self.internal_requests: dict[bytes, asyncio.Future] = dict()
        # TODO: 用于跟踪客户端的请求，如果处理请求的后端服务掉线了，向可用的其他后端服务重发。
        self.cache_request: dict[bytes, tuple] = dict()
        self.running = False
        self._poller = z_aio.Poller()
        self._broker_task: Optional[asyncio.Task] = None
        self.register_internal_process()

    def bind_frontend(self, addr: str, sock_opt: dict = None) -> None:
        """
        绑定前端地址
        """
        if not sock_opt:
            sock_opt = {z_const.IDENTITY: self.identity}
        else:
            sock_opt.update({z_const.IDENTITY: self.identity})
        self.frontend = self.ctx.socket(z_const.ROUTER,z_aio.Socket)
        set_sock(self.frontend, sock_opt)
        self.frontend.bind(check_socket_addr(addr))

    def unbind_frontend(self, addr: str) -> None:
        if self.frontend:
            self.frontend.unbind(check_socket_addr(addr))

    def bind_backend(self, addr: str = None, sock_opt: dict = None) -> None:
        if not addr:
            addr = Settings.WORKER_ADDR
        if not sock_opt:
            sock_opt = {z_const.IDENTITY: self.identity}
        else:
            sock_opt.update({z_const.IDENTITY: self.identity})
        self.backend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        set_sock(self.backend, sock_opt)
        self.backend.bind(check_socket_addr(addr))

    def unbind_backend(self, addr: str) -> None:
        if self.backend:
            self.backend.unbind(check_socket_addr(addr))

    def connect_zap(
        self,
        zap_addr: str = None,
        zap_domain: str = None,
    ):
        """
        :param zap_addr: zap 身份验证服务的地址。
        :param zap_domain: 需要告诉 zap 服务在哪个域验证。
        """
        if zap_domain:
            zap_domain = zap_domain.encode("utf-8")
        else:
            zap_domain = Settings.ZAP_DEFAULT_DOMAIN
        self.zap_domain = zap_domain
        if not zap_addr:
            zap_addr = Settings.ZAP_ADDR
        if zap_addr := check_socket_addr(zap_addr):
            self.zap_addr = zap_addr
            self.zap = self.ctx.socket(z_const.DEALER, z_aio.Socket)
            set_sock(self.zap)
            Mlogger.info(f"Connecting to zap at {zap_addr}")
            self.zap.connect(self.zap_addr)
        else:
            raise ValueError(
                "The ZAP mechanism is specified and "
                "the ZAP server address must also be provided."
            )

    def disconnect_zap(self):
        self.zap_domain = b""
        self.zap.disconnect(self.zap_addr)

    async def disconnect_all(self):
        # TODO: 需要通知客户端吗？
        Mlogger.warning("Disconnected and all the workers.")
        wts = [
            self.now_ban(work)
            for work in self.workers.values()
        ]
        await asyncio.gather(*wts)

    def close(self):
        self.thread_executor.shutdown()
        self.process_executor.shutdown()

    def run(self):
        if not self._broker_task:
            self._broker_task = self._loop.create_task(self._broker_loop())

    def stop(self):
        for internal_task in self.internal_tasks:
            if not internal_task.done():
                internal_task.cancel("AMajordomo Stop.")
        if self._broker_task:
            if not self._broker_task.cancelled():
                self._broker_task.cancel()
            self._broker_task = None
        if self.frontend:
            self._poller.unregister(self.frontend)
            self.frontend.close()
            self.frontend = None
        if self.backend:
            self._poller.unregister(self.backend)
            self.backend.close()
            self.backend = None
        if self.zap:
            self.disconnect_zap()
            self._poller.unregister(self.zap)
            self.zap.close()
            self.zap = None

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
                self.internal_func[name] = {
                    "doc": inspect.getdoc(method),
                    "signature": str(inspect.signature(method))
                }

    def register_service(self, name, description):
        """
        添加 service
        """
        service = self.RemoteServiceClass(
            name, description
        )
        Mlogger.debug(
            f"register service: {service.name}({service.description})"
        )
        self.services[service.name] = service

    async def register_worker(
        self, worker_id, max_allowed_request, sock, service
    ):
        worker = self.RemoteWorkerClass(
            worker_id, self.heartbeat,
            max_allowed_request, sock,
            Settings.MDP_WORKER, service
        )
        worker.prolong(self._loop.time())
        worker.destroy_task = self._loop.create_task(
            self.ban_at_expiration(worker)
        )
        worker.heartbeat_task = self._loop.create_task(
            self.send_heartbeat(worker)
        )
        service.add_worker(worker)
        self.workers[worker.identity] = worker

    async def ban_worker(self, worker: RemoteWorker):
        """
        屏蔽掉指定 worker
        """
        Mlogger.warning(f"ban worker({worker.identity}).")
        if worker.is_alive():
            await self.send_to_backend(
                worker,
                Settings.MDP_COMMAND_DISCONNECT
            )
            worker.ready = False
            worker.heartbeat_task.cancel()
        worker.service.remove_worker(worker)
        self.workers.pop(worker.identity, None)
        return True

    async def ban_at_expiration(self, worker: RemoteWorker):
        while 1:
            if worker.expiry <= self._loop.time():
                Mlogger.debug(f"ban worker({worker.identity}) at expiration")
                await self.ban_worker(worker)
                return
            await asyncio.sleep(
                1e-3 * worker.heartbeat * worker.heartbeat_liveness
            )

    async def now_ban(self, worker: RemoteWorker):
        Mlogger.debug(f"now ban worker({worker.identity})")
        if worker.destroy_task:
            worker.destroy_task.cancel()
        await self.ban_worker(worker)

    async def delay_ban(self, worker: RemoteWorker):
        worker.prolong(self._loop.time())

    async def send_heartbeat(self, worker: RemoteWorker):
        while 1:
            time_from_last_message = (
                    self._loop.time() - worker.last_message_sent_time
            )
            if time_from_last_message >= 1e-3 * worker.heartbeat:
                await self.send_to_backend(
                    worker,
                    Settings.MDP_COMMAND_HEARTBEAT
                )
                await asyncio.sleep(1e-3 * worker.heartbeat)
            else:
                await asyncio.sleep(
                    1e-3 * worker.heartbeat - time_from_last_message
                )

    def cleanup_frontend_task(self, task: asyncio.Task):
        self.frontend_tasks.remove(task)
        if exception := task.exception():
            exception_info = ''.join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Mlogger.warning(f"frontend task exception:\n{exception_info}")

    def cleanup_backend_task(self, task: asyncio.Task):
        self.backend_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Mlogger.warning(f"backend task exception:\n{exception_info}")

    def create_internal_task(
        self, coro_func: Callable[..., Coroutine], name=None, reload=False,
        func_args=None, func_kwargs=None
    ):
        if Settings.DEBUG > 1:
            Mlogger.debug(
                f"create internal task<{coro_func} name={name} reload={reload} "
                f"func_args={func_args} func_kwargs={func_kwargs}"
            )
        if func_args is None:
            func_args = tuple()
        if func_kwargs is None:
            func_kwargs = dict()
        task = self._loop.create_task(
            coro_func(*func_args, **func_kwargs), name=name
        )
        self.internal_tasks[task] = (
            coro_func, name, reload, func_args, func_kwargs
        )
        task.add_done_callback(self.cleanup_internal_task)

    def cleanup_internal_task(self, task: asyncio.Task):
        coro_func, name, reload, args, kwargs = self.internal_tasks.pop(task)
        if exception := task.exception():
            exception_info = ''.join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Mlogger.warning(f"internal task exception:\n{exception_info}")
            if isinstance(exception, asyncio.CancelledError):
                return
        if reload:
            if Settings.DEBUG:
                Mlogger.debug(f"recreate task from {coro_func}")
            self.create_internal_task(
                coro_func, name, reload, args, kwargs
            )

    async def internal_interface(
        self, client_id: bytes, request_id: bytes, func_name, args, kwargs
    ):
        if client_id not in self.clients:
            if Settings.DEBUG > 2:
                raise RuntimeError(f"client_id({client_id}) not in clients")
            return
        reply = None
        exception = None
        kwargs.update({
            "__client_id": client_id,
            "__request_id": request_id,
        })
        try:
            if not self.internal_func.get(func_name, None):
                if Settings.SECURE:
                    return
                raise AttributeError(
                    "Function not found: {0}".format(func_name)
                )
            func = getattr(self, func_name)
            if Settings.DEBUG > 1:
                Mlogger.debug(
                    f"internal process, "
                    f"func_name: {func_name}, "
                    f"args: {args}, "
                    f"kwargs: {kwargs}"
                )
            if inspect.iscoroutinefunction(func):
                reply = await func(*args, **kwargs)
            else:
                if func.__executor__ == "thread":
                    reply = await run_in_executor(
                        self._loop, self.thread_executor,
                        func, *args, **kwargs
                    )
                elif func.__executor__ == "process":
                    reply = await run_in_executor(
                        self._loop, self.process_executor,
                        func, *args, **kwargs
                    )
                else:
                    reply = func(*args, **kwargs)
        except Exception as error:
            exception = error
        reply = await generate_reply(result=reply, exception=exception)
        if isinstance(reply, HugeData):
            task = self._loop.create_task(
                self.reply_hugedata(
                    client_id, request_id, self.reply_frontend, reply
                ),
                name=f"MajordomoReplyCliHD-{client_id}-{request_id}"
            )
        elif isinstance(reply, AsyncGenerator):
            task = self._loop.create_task(
                self.reply_agenerator(
                    client_id, request_id, self.reply_frontend, reply
                ),
                name=f"MajordomoReplyCliAG-{client_id}-{request_id}"
            )
        else:
            task = self._loop.create_task(
                self.reply_frontend(
                    client_id, Settings.MDP_INTERNAL_SERVICE,
                    request_id, msg_pack(reply)
                )
            )
        self.frontend_tasks.add(task)
        task.add_done_callback(self.cleanup_frontend_task)

    async def reply_hugedata(self, client, request_id, reply_func, hugedata):
        try:
            async for data in hugedata.compress(self._loop):
                await reply_func(
                    client,
                    Settings.MDP_INTERNAL_SERVICE,
                    request_id,
                    Settings.STREAM_HUGE_DATA_TAG,
                    data
                )
        except Exception as e:
            await reply_func(
                client,
                Settings.MDP_INTERNAL_SERVICE,
                request_id,
                Settings.STREAM_HUGE_DATA_TAG,
                hugedata.except_tag
            )
            e = msg_pack(
                (
                    e.__class__.__name__,
                    e.__repr__(),
                    "".join(format_tb(e.__traceback__))
                )
            )
            await reply_func(
                client,
                Settings.MDP_INTERNAL_SERVICE,
                request_id,
                Settings.STREAM_HUGE_DATA_TAG,
                e
            )
        await reply_func(
            client,
            Settings.MDP_INTERNAL_SERVICE,
            request_id,
            Settings.STREAM_HUGE_DATA_TAG,
            hugedata.end_tag
        )
        hugedata.destroy()

    async def reply_agenerator(
        self, client, request_id, reply_func, agenerator: AsyncGenerator
    ):
        try:
            async for sub_reply in agenerator:
                messages = msg_pack(sub_reply)
                await reply_func(
                    client,
                    Settings.MDP_INTERNAL_SERVICE,
                    request_id,
                    Settings.STREAM_GENERATOR_TAG,
                    messages
                )
            await reply_func(
                client,
                Settings.MDP_INTERNAL_SERVICE,
                request_id,
                Settings.STREAM_GENERATOR_TAG,
                Settings.STREAM_END_TAG
            )
        except Exception as e:
            e = msg_pack(
                (
                    e.__class__.__name__,
                    e.__repr__(),
                    "".join(format_tb(e.__traceback__))
                )
            )
            await reply_func(
                client,
                Settings.MDP_INTERNAL_SERVICE,
                request_id,
                Settings.STREAM_EXCEPT_TAG,
                Settings.STREAM_EXCEPT_TAG,
                e
            )

    async def request_zap(
        self, identity: bytes, address: bytes, request_id: bytes,
        mechanism: bytes, credentials: tuple
    ) -> bool:
        cache_key = (address, mechanism, *credentials)
        try:
            return self.zap_cache[cache_key]
        except KeyError:
            pass
        reply_key = request_id + b"-" + uuid4().bytes
        zap_frames = (
            b"",  # 空帧
            Settings.ZAP_VERSION,
            reply_key,
            self.zap_domain,  # 域
            address,  # IP地址
            identity,  # 身份id
            mechanism,
            *credentials
        )
        zap_frames = tuple(to_bytes(s) for s in zap_frames)
        self.zap_replies[reply_key] = asyncio.Future()
        failed_reply = {
            "user_id": identity,
            "request_id": request_id,
            "metadata": b"",
            "status_code": b"500",
            "status_text": b"ZAP Service Failed.",
        }
        try:
            await self.zap.send_multipart(zap_frames)
        except asyncio.TimeoutError:
            Mlogger.warning(f"send ZAP request failed: {zap_frames}.")
            zap_reply = failed_reply
        else:
            try:
                zap_reply = await asyncio.wait_for(
                    self.zap_replies[reply_key],
                    Settings.ZAP_REPLY_TIMEOUT
                )
                self.zap_replies.pop(reply_key).cancel()
            except asyncio.TimeoutError:
                Mlogger.warning(f"get ZAP replies failed: {reply_key}")
                zap_reply = failed_reply
        if zap_reply["status_code"] != b"200":
            Mlogger.warning(
                f"Authentication failure: {zap_reply['status_code']}"
                f"({zap_reply['status_text']})"
            )
            self.zap_cache[cache_key] = False
            return False
        self.zap_cache[cache_key] = True
        return True

    async def process_replies_from_zap(self, replies):
        """
        当使用 DEALER 类套接字时，需要异步获取验证结果。
        使用 asyncio.future 异步设置结果。
        """
        try:
            (
                empty_frame,
                zap_version,
                reply_key,
                status_code,
                status_text,
                user_id,
                metadata
            ) = replies
        except ValueError:
            return
        request_id = reply_key.split(b"-")[0]
        future = self.zap_replies[reply_key]
        try:
            future.set_result(
                {
                    "user_id": user_id,
                    "request_id": request_id,
                    "metadata": metadata,
                    "status_code": status_code,
                    "status_text": status_text,
                }
            )
        except asyncio.InvalidStateError as e:
            Mlogger.warning(
                f"zap_replies({reply_key}) set result failed: {e}, "
                f"done: {future.done()}, cancelled: {future.cancelled()}"
            )

    async def process_ready_command(self, worker_id, service_info):
        try:
            service_name, service_description, max_allowed_request = (
                from_bytes(service_info)
            ).split(Settings.MDP_DESCRIPTION_SEP)
            max_allowed_request = int(max_allowed_request)
        except ValueError:
            return
        if service_name not in self.services:
            self.register_service(service_name, service_description)
        service = self.services[service_name]
        await self.register_worker(
            worker_id, max_allowed_request,
            self.backend, service
        )

    async def process_heartbeat_command_from_backend(self, worker):
        if not worker.ready:
            return await self.now_ban(worker)
        await self.delay_ban(worker)

    async def process_reply_command_from_backend(self, service_name, messages):
        try:
            client_id, empty_frame, request_id, *body = messages
            if empty_frame:
                return
        except ValueError:
            return
        if client_id == self.identity:
            if request_id in self.internal_requests:
                self.internal_requests[request_id].set_result(body)
        elif client_id in self.clients:
            self.frontend_tasks.add(
                task := self._loop.create_task(
                    self.reply_frontend(
                        client_id, service_name, request_id, *body
                    )
                )
            )
            task.add_done_callback(self.cleanup_frontend_task)

    async def process_disconnect_command_from_backend(self, worker):
        worker.ready = False
        worker.expiry = self._loop.time()
        Mlogger.warning(
            f"recv disconnect and ban worker({worker.identity})"
        )
        await self.now_ban(worker)

    async def process_messages_from_backend(self, messages: list):
        try:
            (
                worker_id,
                empty_frame,
                mdp_version,
                command,
                *messages
            ) = messages
            if (
                    empty_frame
                    or mdp_version != Settings.MDP_WORKER
                    or command not in (
                        Settings.MDP_COMMAND_READY,
                        Settings.MDP_COMMAND_HEARTBEAT,
                        Settings.MDP_COMMAND_REQUEST,
                        Settings.MDP_COMMAND_REPLY,
                        Settings.MDP_COMMAND_DISCONNECT
                    )
            ):
                return
        except ValueError:
            return

        if Settings.DEBUG > 1:
            Mlogger.debug(
                f"worker id: {worker_id}\n"
                f"command: {command}\n"
                f"messages: {messages}\n"
            )

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
                request_id = b"worker_" + command
                zap_result = await self.request_zap(
                    worker_id, worker_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    if Settings.DEBUG:
                        Mlogger.warning(
                            f"ZAP verification of Worker({worker_id})"
                            f" fails,'{mechanism}' mechanism is used, and the "
                            f"credential is '{credentials}', and the request_id "
                            f"is '{request_id}'"
                        )
                    return
            else:
                return
        if not (worker := self.workers.get(worker_id, None)):
            Mlogger.warning(f"The worker({worker_id}) was not registered.")

        # MDP 定义的 Worker 命令
        if command == Settings.MDP_COMMAND_READY and len(messages) == 1:
            if worker:
                return await self.now_ban(worker)
            return await self.process_ready_command(
                worker_id, messages[0]
            )

        elif command == Settings.MDP_COMMAND_HEARTBEAT and worker and not messages:
            return await self.process_heartbeat_command_from_backend(worker)

        elif command == Settings.MDP_COMMAND_REPLY and worker:
            if not worker.ready:
                return await self.now_ban(worker)

            await self.process_reply_command_from_backend(
                worker.service.name, messages
            )
            service = worker.service
            await service.release_worker(worker)
            await self.delay_ban(worker)
            return

        elif (
                command == Settings.MDP_COMMAND_DISCONNECT
                and worker
                and len(messages) == 1
        ):
            return await self.process_disconnect_command_from_backend(worker)

    async def request_backend(
        self,
        service_name: str,
        client_id: bytes,
        request_id: bytes,
        body: Union[
            list[Union[bytes, str], ...], tuple[Union[bytes, str], ...]
        ],
        cache=False,
    ):
        service = self.services.get(service_name)
        try:
            if not service:
                raise ServiceUnAvailableError(service_name)
            worker = await service.acquire_idle_worker()
            option = (client_id, b"", request_id)
            self.backend_tasks.add(
                task := self._loop.create_task(
                    self.send_to_backend(
                        worker, Settings.MDP_COMMAND_REQUEST,
                        option, body
                    )
                )
            )
            task.add_done_callback(self.cleanup_backend_task)
            if client_id == self.identity:
                self.internal_requests[request_id] = self._loop.create_future()
        except (ServiceUnAvailableError, BusyWorkerError) as exception:
            if client_id in self.clients:
                # TODO: 在服务再次可用时，重试请求，需要吗？
                # if cache:
                #     self.cache_request[request_id] = (
                #         service_name, client_id, request_id, body, self._loop.time()
                #     )
                exception = await generate_reply(exception=exception)
                except_reply = msg_pack(exception)
                self.frontend_tasks.add(
                    task := self._loop.create_task(
                        self.reply_frontend(
                            client_id, service_name, request_id,
                            except_reply
                        )
                    )
                )
                task.add_done_callback(self.cleanup_frontend_task)
            raise exception

    async def send_to_backend(
        self,
        worker: RemoteWorker,
        command: bytes,
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[list[Union[bytes, str], ...], tuple[Union[bytes, str], ...]] = None
    ):
        if isinstance(messages, (bytes, str)):
            messages = (messages,)
        elif not messages:
            messages = tuple()
        if option:
            messages = (*option, *messages)
        messages = (
            worker.identity,
            b"",
            Settings.MDP_WORKER,
            command,
            *messages
        )
        request = tuple(to_bytes(frame) for frame in messages)
        await worker.sock.send_multipart(request)
        self.workers[worker.identity].last_message_sent_time = self._loop.time()

    async def process_messages_from_frontend(self, request: list):
        try:
            (
                client_id,
                empty_frame,
                mdp_version,
                service_name,
                *messages
            ) = request
            if empty_frame or mdp_version != Settings.MDP_CLIENT:
                return
        except ValueError:
            return

        if Settings.DEBUG > 1:
            Mlogger.debug(
                f"client id: {client_id}\n"
                f"service_name: {service_name}\n"
                f"messages: {messages}\n"
            )

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
            else:
                return
        else:
            mechanism = credentials = None
        try:
            request_id, *body = messages
        except ValueError:
            return
        if mechanism:
            zap_result = await self.request_zap(
                client_id, client_id, request_id,
                mechanism, credentials
            )
            if not zap_result:
                return
        self.clients.add(client_id)
        service_name = from_bytes(service_name)
        if service_name == Settings.MDP_INTERNAL_SERVICE:
            func_name, *body = body
            func_name = msg_unpack(func_name)
            if not body:
                args, kwargs = tuple(), dict()
            elif len(body) == 1:
                args, kwargs = msg_unpack(body[0]), dict()
            elif len(body) == 2:
                args, kwargs = (msg_unpack(b) for b in body)
            else:
                return
            self.create_internal_task(
                self.internal_interface,
                name=f"InternalFun({func_name})-{client_id}-{request_id}",
                func_args=(client_id, request_id, func_name, args, kwargs)
            )
        else:
            self.backend_tasks.add(
                task := self._loop.create_task(
                    self.request_backend(
                        service_name, client_id, request_id, body
                    )
                )
            )
            task.add_done_callback(self.cleanup_backend_task)

    async def reply_frontend(
        self, client_id: bytes,
        service_name: Union[str, bytes],
        request_id: Union[str, bytes],
        *body
    ):
        if Settings.DEBUG > 1:
            Mlogger.debug(
                f"client id: {client_id}\n"
                f"request id: {request_id}\n"
                f"service_name: {service_name}\n"
                f"body: {body}\n"
            )
        if client_id in self.clients:
            reply = (
                client_id,
                b"",
                Settings.MDP_CLIENT,
                service_name,
                request_id,
                *body
            )
            reply = tuple(to_bytes(frame) for frame in reply)
            await self.frontend.send_multipart(reply)

    async def _broker_loop(self):
        try:
            if self.frontend:
                self._poller.register(self.frontend, z_const.POLLIN)
            if self.backend:
                self._poller.register(self.backend, z_const.POLLIN)
            if self.zap:
                self._poller.register(self.zap, z_const.POLLIN)
            self.running = True

            while 1:
                socks = await self._poller.poll(self.heartbeat)
                socks = dict(socks)
                if self.backend and self.backend in socks:
                    replies = await self.backend.recv_multipart()
                    bt = self._loop.create_task(
                        self.process_messages_from_backend(replies),
                    )
                    self.backend_tasks.add(bt)
                    bt.add_done_callback(self.cleanup_backend_task)

                if self.frontend and self.frontend in socks:
                    replies = await self.frontend.recv_multipart()
                    # 同后端任务处理。
                    ft = self._loop.create_task(
                        self.process_messages_from_frontend(replies),
                    )
                    self.frontend_tasks.add(ft)
                    ft.add_done_callback(self.cleanup_frontend_task)

                if self.zap and self.zap in socks:
                    replies = await self.zap.recv_multipart()
                    zt = self._loop.create_task(
                        self.process_replies_from_zap(replies),
                    )
                    self.zap_tasks.add(zt)
                    zt.add_done_callback(self.zap_tasks.remove)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Mlogger.error(exception)
            raise
        finally:
            await self.disconnect_all()
            Mlogger.info("broker loop exit.")
            self.close()

    @interface
    def info(self, **kwargs):
        return {
            "my_id": self.identity,
            "services": list(self.services.keys())
        }

    @interface
    def query_service(self, service_name, **kwargs):
        if service_name in self.services:
            return self.services[service_name].description
        else:
            return None

    @interface
    def get_services(self, **kwargs):
        services = list(self.services.keys())
        services.append(Settings.MDP_INTERNAL_SERVICE)
        return services

    @interface
    def keepalive(self, heartbeat, **kwargs):
        if heartbeat == b"heartbeat":
            return b"200"
        else:
            return b"400"


class RemoteGate(RemoteWorker):

    def __init__(
        self,
        identity: bytes,
        heartbeat: int,
        sock: z_aio.Socket,
        member_version: bytes,
        cluster: "GateClusterService",
        active_services: dict[str, int],
    ):
        """
        远程 GateRPC 的本地映射
        :param identity: Gate ID
        :param heartbeat: 心跳间隔
        :param member_version: 成员协议版本
        :param cluster: GateCluster 服务实例
        :param active_services: 工作服务
        """
        super().__init__(
            identity, heartbeat,
            Settings.MESSAGE_MAX, sock, member_version,
            cluster
        )
        self.active_services = active_services


class GateClusterService(ABCService):

    def __init__(self, cluster_name, local_gate: "Gate"):
        self.name = cluster_name
        self.identity = name2uuid(cluster_name).bytes
        self.local_node = local_gate
        self.description = Settings.GATE_CLUSTER_DESCRIPTION
        self.services: dict[str, asyncio.Queue[bytes]] = dict()
        self.workers: dict[bytes, RemoteGate] = dict()
        self.idle_workers: asyncio.Queue[bytes] = asyncio.Queue()
        self.unready_gates: dict[bytes, z_aio.Socket] = dict()

    def add_worker(self, gate: Optional[RemoteGate]) -> None:
        if gate.identity in self.unready_gates:
            self.unready_gates.pop(gate.identity, None)
        self.workers[gate.identity] = gate
        for service in gate.active_services:
            if service not in self.services:
                self.services[service] = asyncio.Queue()
            self.services[service].put_nowait(gate.identity)
        self.idle_workers.put_nowait(gate.identity)
        self.running = True

    def remove_worker(self, gate: Optional[RemoteGate]) -> None:
        if gate.identity in self.workers:
            self.workers.pop(gate.identity)

    def cleanup_unready_gate(self, gate_id):
        if Settings.DEBUG:
            Glogger.debug(f"Cleanup unready gate({gate_id})")
        if sock := self.unready_gates.pop(gate_id, None):
            try:
                sock.close()
            except Exception as e:
                Glogger.error(f"Failed to close the temporary connection: {e}")

    def get_workers(self) -> dict[bytes, Union[RemoteGate]]:
        return {
            g_id: gate for g_id, gate in self.workers.items()
            if gate.ready
        }

    def online_nodes(self) -> list[bytes]:
        return [self.local_node.identity, *self.get_workers().keys()]

    def all_nodes(self):
        nodes = {
            self.local_node.identity, *self.workers.keys(), 
            *self.unready_gates.keys()
        }
        return list(nodes)

    async def acquire_idle_worker(self) -> RemoteGate:
        if not self.running:
            raise ServiceUnAvailableError(self.name)
        return await self.acquire_service_idle_worker(self.name)

    async def acquire_service_idle_worker(
        self, service: str
    ) -> RemoteGate:
        if service == self.name:
            idle_gates = self.idle_workers
        else:
            if not (idle_gates := self.services.get(service)):
                raise ServiceUnAvailableError(service)
        while 1:
            try:
                gate_id = await asyncio.wait_for(idle_gates.get(), Settings.TIMEOUT)
            except asyncio.TimeoutError:
                raise BusyGateError()
            try:
                gate = self.workers[gate_id]
                if gate.is_alive() and gate.active_services[service] != 0:
                    gate.active_services[service] -= 1
                    if gate.active_services[service] > 0:
                        await idle_gates.put(gate_id)
                    return gate
            except KeyError:
                continue

    async def release_worker(self, gate: RemoteGate):
        return await self.release_service_worker(self.name, gate)

    async def release_service_worker(self, service: str, gate: RemoteGate):
        if gate.identity in self.workers and gate.is_alive():
            if service == self.name:
                idle_gates = self.idle_workers
            else:
                idle_gates = self.services[service]
            if gate.active_services[service] >= 0:
                await idle_gates.put(gate.identity)
            gate.active_services[service] += 1


class Gate(AMajordomo):
    GateServiceClass = GateClusterService
    RemoteGateClass = RemoteGate

    def __init__(
        self,
        port: int,
        *,
        identity: str = None,
        context: Context = None,
        heartbeat: int = None,
        multicast: bool = True,
        cluster_name: str = None,
        gate_zap_mechanism: Union[str, bytes] = None,
        gate_zap_credentials: tuple = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ):
        if not identity:
            identity = f"Gate-{secrets.token_hex(8)}"
        Glogger.debug(f"Gate: {identity}")
        if not context:
            context = Context()
        gate_zap_mechanism, gate_zap_credentials = check_zap_args(
            gate_zap_mechanism, gate_zap_credentials
        )
        if gate_zap_mechanism:
            self.gate_zap_frames = [gate_zap_mechanism, *gate_zap_credentials]
        else:
            self.gate_zap_frames = None
        super().__init__(
            identity=identity, context=context, heartbeat=heartbeat,
            thread_executor=thread_executor, process_executor=process_executor
        )
        # gate
        self.gate: Optional[z_aio.Socket] = None
        self.gate_port = port
        if not cluster_name:
            cluster_name = Settings.GATE_CLUSTER_NAME
        self.gate_cluster = self.GateServiceClass(cluster_name, self)
        # 通知事件和对应的处理函数
        self.event_handlers: dict[str, Callable[RemoteGate, ..., Coroutine]] = dict()
        self.gate_replies: dict[bytes, asyncio.Future] = dict()
        # 被转发的客户端请求和其他代理的请求和客户端id或其他地理的id的映射
        self.request_map: dict[bytes, bytes] = dict()
        self.gate_tasks = set()
        self.bind_gate()
        self.multicast = False
        self.mcast_sock = None
        self.mcast_msg = b"%s%s%s%s%s" % (
            self.identity,
            Settings.GATE_MEMBER,
            Settings.GATE_COMMAND_NOTICE,
            self.gate_cluster.identity,
            to_bytes(self.gate_port, fmt="!i"),
        )
        if multicast:
            self.multicast = True
        self.register_event_handler(
            "UpdateService", self.update_service
        )

    def bind_gate(self, sock_opt: dict = None) -> None:
        if not sock_opt:
            sock_opt = {z_const.IDENTITY: self.identity}
        else:
            sock_opt.update({z_const.IDENTITY: self.identity})
        if Settings.GATE_CURVE_KEY:
            sock_opt.update({
                z_const.CURVE_SECRETKEY: Settings.GATE_CURVE_KEY,
                z_const.CURVE_SERVER: True,
            })

        self.gate = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        set_sock(self.gate, sock_opt)
        self.gate.bind(f"tcp://*:{self.gate_port}")

    def unbind_gate(self, addr: str) -> None:
        if self.gate:
            self.gate.unbind(check_socket_addr(addr))

    async def connect_gate(
        self, gate_id, addr: str, sock_opt: dict = None
    ) ->  None:
        Glogger.debug(f"Connect Gate({addr})")
        # self.gate.connect(addr)
        # self.gate_cluster.unready_gates[gate_id] = addr
        # self._loop.call_later(
        #     1e-3 * self.heartbeat,
        #     self.cleanup_unready_gate, gate_id
        # )
        if gate_id in self.gate_cluster.workers:
            raise RuntimeError(f"Gate has already been connected.")
        if not (sock := self.gate_cluster.unready_gates.get(gate_id)):
            if not sock_opt:
                sock_opt = {z_const.IDENTITY: self.identity}
            else:
                sock_opt.update({z_const.IDENTITY: self.identity})
            if Settings.GATE_CURVE_PUBKEY:
                _pub, _pri = curve_keypair()
                sock_opt.update({
                    z_const.CURVE_SECRETKEY: _pri,
                    z_const.CURVE_PUBLICKEY: _pub,
                    z_const.CURVE_SERVERKEY: Settings.GATE_CURVE_PUBKEY,
                })
            sock = self.ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
            set_sock(sock, sock_opt)
            sock.connect(addr)
            self._loop.call_later(
                self.heartbeat * Settings.MDP_HEARTBEAT_INTERVAL / 1000,
                self.gate_cluster.cleanup_unready_gate,
                gate_id
            )
            self.gate_cluster.unready_gates[gate_id] = sock

        await asyncio.sleep(0)
        if Settings.DEBUG > 1:
            Glogger.debug(f"Invite Gate({gate_id})")
        await self.send_invite_command(sock)

    async def disconnect_all(self):
        await super().disconnect_all()
        gts = [
            self.now_ban_gate(gate)
            for gate in self.gate_cluster.get_workers().values()
        ]
        await asyncio.gather(*gts)

    def run(self):
        super().run()
        if self.multicast:
            self.create_internal_task(
                self._multicast_self, "multicast_self", True
            )

    def stop(self):
        super().stop()
        if self.mcast_sock:
            self.mcast_sock.close()
            self.mcast_sock = None
        if self.gate:
            self._poller.unregister(self.gate)
            self.gate.close()
            self.gate = None

    def register_event_handler(self, name, coro_func):
        self.event_handlers[name] = coro_func

    async def process_ready_command(self, worker_id, service_info):
        await super().process_ready_command(worker_id, service_info)
        service = self.workers[worker_id].service
        self.gate_tasks.add(
            task := self._loop.create_task(
                self.send_notice_command(
                    uuid4().bytes,
                    "UpdateActiveService",
                    (
                        msg_pack(service.name),
                        msg_pack(sum(
                            worker.max_allowed_request
                            for worker in service.workers.values()
                        ))
                    )
                )
            )
        )
        task.add_done_callback(self.cleanup_gate_task)

    async def internal_interface(
        self, client_id: bytes, request_id: bytes, func_name, args, kwargs
    ):
        if client_id in self.clients:
            return await super().internal_interface(
                client_id, request_id, func_name, args, kwargs
            )
        elif not (gate := self.gate_cluster.get_workers().get(client_id)):
            if Settings.DEBUG > 2:
                raise RuntimeError(
                    f"client_id({client_id}) not in clients or gates"
                )
            return
        reply = None
        exception = None
        kwargs.update(
            {
                "client_id": client_id,
                "request_id": request_id,
            }
        )
        try:
            if not self.internal_func.get(func_name, None):
                if Settings.SECURE:
                    return
                raise AttributeError(
                    "Function not found: {0}".format(func_name)
                )
            func = getattr(self, func_name)
            if Settings.DEBUG > 1:
                Glogger.debug(
                    f"internal process, "
                    f"func_name: {func_name}, "
                    f"args: {args}, "
                    f"kwargs: {kwargs}"
                )
            if inspect.iscoroutinefunction(func):
                reply = await func(*args, **kwargs)
            else:
                if func.__executor__ == "thread":
                    reply = await run_in_executor(
                        self._loop, self.thread_executor,
                        func, *args, **kwargs
                    )
                elif func.__executor__ == "process":
                    reply = await run_in_executor(
                        self._loop, self.process_executor,
                        func, *args, **kwargs
                    )
                else:
                    reply = func(*args, **kwargs)
        except Exception as error:
            exception = error
        reply = await generate_reply(result=reply, exception=exception)
        if isinstance(reply, HugeData):
            task = self._loop.create_task(
                self.reply_hugedata(
                    gate, request_id, self.reply_gate, reply
                ),
                name=f"GateReplyGateHD-{client_id}-{request_id}"
            )
        elif isinstance(reply, AsyncGenerator):
            task = self._loop.create_task(
                self.reply_agenerator(
                    gate, request_id, self.reply_gate, reply
                ),
                name=f"GateReplyGateAG-{client_id}-{request_id}"
            )
        else:
            task = self._loop.create_task(
                self.reply_gate(
                    gate, Settings.MDP_INTERNAL_SERVICE,
                    request_id, msg_pack(reply)
                )
            )
        self.gate_tasks.add(task)
        task.add_done_callback(self.cleanup_gate_task)

    async def process_reply_command_from_backend(self, service_name, messages):
        try:
            client_id, empty_frame, request_id, *body = messages
            if empty_frame:
                return
        except ValueError:
            return
        if gate := self.gate_cluster.get_workers().get(client_id):
            self.gate_tasks.add(
                task := self._loop.create_task(
                    self.reply_gate(
                        gate, service_name, request_id, *body
                    )
                )
            )
            task.add_done_callback(self.cleanup_gate_task)
        else:
            await super().process_reply_command_from_backend(
                service_name, messages
            )

    async def forward_request(
        self, service_name, request_id, client_id, body
    ):
        if Settings.DEBUG > 1:
            Glogger.warning(
                f"Service({service_name}) unavailable, forward request"
            )
        # 转发请求其他代理。
        self.request_map[request_id] = client_id
        if not self.gate_cluster.get_workers():
            raise GateUnAvailableError()
        gate = await self.gate_cluster.acquire_service_idle_worker(service_name)
        self.gate_tasks.add(
            task := self._loop.create_task(
                self.send_whisper_command(
                    gate,
                    request_id,
                    service_name,
                    body
                )
            )
        )
        task.add_done_callback(self.cleanup_gate_task)

    async def request_backend(
        self,
        service_name: str,
        client_id: bytes,
        request_id: bytes,
        body: Union[
            list[Union[bytes, str], ...], tuple[Union[bytes, str], ...]
        ],
        cache=False
    ):
        try:
            try:
                if not (service := self.services.get(service_name)):
                    raise ServiceUnAvailableError(service_name)
                worker = await service.acquire_idle_worker()
                option = (client_id, b"", request_id)
                self.backend_tasks.add(
                    task := self._loop.create_task(
                        self.send_to_backend(
                            worker, Settings.MDP_COMMAND_REQUEST,
                            option, body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_backend_task)
            except (ServiceUnAvailableError, BusyWorkerError) as exception:
                if service_name not in self.gate_cluster.services:
                    raise exception
                await self.forward_request(
                    service_name, request_id, client_id, body
                )
            if client_id == self.identity:
                self.internal_requests[request_id] = self._loop.create_future()
        except (
                ServiceUnAvailableError, GateUnAvailableError,
                BusyWorkerError, BusyGateError
        ) as exception:
            except_reply = await generate_reply(exception=exception)
            except_reply = msg_pack(except_reply)
            if Settings.DEBUG > 1:
                Glogger.error(except_reply)
            if gate := self.gate_cluster.get_workers().get(client_id):
                Glogger.debug("reply gate")
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.reply_gate(
                            gate, service_name, request_id,
                            except_reply
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)
            elif client_id in self.clients:
                Glogger.debug("reply client")
                self.frontend_tasks.add(
                    task := self._loop.create_task(
                        self.reply_frontend(
                            client_id, service_name, request_id,
                            except_reply
                        )
                    )
                )
                task.add_done_callback(self.cleanup_frontend_task)
            raise exception

    async def build_gate_zap_frames(self) -> tuple[bytes, ...]:
        """
        用于继承后根据 ZAP 校验方法构建 ZAP 凭据帧，
        使用协程是为了方便使用 loop.run_in_executor
        """
        if self.gate_zap_frames:
            return tuple(to_bytes(z) for z in self.gate_zap_frames)
        else:
            return tuple()

    async def register_gate(self, gate_id, active_services):
        if Settings.DEBUG:
            Glogger.info(f"register Gate({gate_id})")
        sock = self.gate_cluster.unready_gates[gate_id]
        gate = self.RemoteGateClass(
            gate_id, self.heartbeat,
            sock, Settings.GATE_MEMBER,
            self.gate_cluster,
            active_services
        )
        gate.prolong(self._loop.time())
        gate.destroy_task = self._loop.create_task(
            self.ban_gate_at_expiration(gate)
        )
        gate.heartbeat_task = self._loop.create_task(
            self.send_gate_heartbeat(gate)
        )
        self.gate_cluster.add_worker(gate)

    async def ban_gate(self, gate: RemoteGate):
        """
        和 ban_worker 分开，方便以后加逻辑
        """
        Glogger.warning(f"ban gate({gate.identity}).")
        if gate.is_alive():
            await self.send_to_gate(
                gate,
                Settings.GATE_COMMAND_LEAVE,
            )
            gate.ready = False
            gate.heartbeat_task.cancel()
        self.gate_cluster.remove_worker(gate)
        Glogger.warning(f"gate({gate.identity}).sock.close()")
        gate.sock.close()
        return True

    async def ban_gate_at_expiration(self, gate: RemoteGate):
        while 1:
            if gate.expiry <= self._loop.time():
                Glogger.debug(f"ban gate({gate.identity}) at expiration")
                await self.ban_gate(gate)
                return
            await asyncio.sleep(
                1e-3 * gate.heartbeat * gate.heartbeat_liveness
            )

    async def now_ban_gate(self, gate: RemoteGate):
        Glogger.debug(f"now ban gate({gate.identity})")
        if gate.destroy_task:
            gate.destroy_task.cancel()
        await self.ban_gate(gate)

    async def delay_ban_gate(self, gate: RemoteGate):
        gate.prolong(self._loop.time())

    async def send_gate_heartbeat(self, gate: RemoteGate):
        while 1:
            if self.mcast_sock:
                await asyncio.sleep(1e-3 * gate.heartbeat)
            else:
                time_from_last_message = (
                        self._loop.time() - gate.last_message_sent_time
                )
                if time_from_last_message >= 1e-3 * gate.heartbeat:
                    await self.send_to_gate(
                        gate,
                        Settings.GATE_COMMAND_HEARTBEAT
                    )
                    await asyncio.sleep(1e-3 * gate.heartbeat)
                else:
                    await asyncio.sleep(
                        1e-3 * gate.heartbeat - time_from_last_message
                    )

    def cleanup_gate_task(self, task: asyncio.Task):
        self.gate_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Glogger.warning(f"gate task exception:\n{exception_info}")

    async def send_invite_command(self, sock):
        await self.send_to_gate(
            sock,
            Settings.GATE_COMMAND_INVITE
        )

    async def process_invite_command(self, gate_id):
        await self.send_join_command(gate_id)

    async def send_join_command(self, gate_id):
        if not (
                (gate_sock := self.gate_cluster.unready_gates.get(gate_id))
            or (gate_sock := self.gate_cluster.workers.get(gate_id))
        ):
            if not Settings.SECURE:
                Glogger.error(f"Not connected to the {gate_id}.")
            return
        if Settings.DEBUG > 1:
            Glogger.debug(f"Join to Gate({gate_id})")
        services = {
            name: sum(
                worker.max_allowed_request
                for worker in service.workers.values()
            )
            for name, service in self.services.items()
        }
        await self.send_to_gate(
            gate_sock,
            Settings.GATE_COMMAND_JOIN,
            messages=msg_pack(services)
        )

    async def process_join_command(self, gate_id, active_services):
        await self.register_gate(gate_id, active_services)

    async def send_whisper_command(
        self, gate, request_id, service_name, body
    ):
        if Settings.DEBUG > 1:
            Glogger.debug(f"Send WHISPER to {gate.identity}")
        option = (service_name, request_id)
        await self.send_to_gate(
            gate,
            Settings.GATE_COMMAND_WHISPER,
            option,
            body
        )

    async def process_gate_request(
        self, gate_id, request_id, service_name, body
    ):
        service_name = from_bytes(service_name)
        if service_name == Settings.MDP_INTERNAL_SERVICE:
            func_name, *body = body
            func_name = msg_unpack(func_name)
            if not body:
                args, kwargs = tuple(), dict()
            elif len(body) == 1:
                args, kwargs = msg_unpack(body[0]), dict()
            elif len(body) == 2:
                args, kwargs = (msg_unpack(b) for b in body)
            else:
                return
            self.create_internal_task(
                self.internal_interface,
                name=f"InternalFunc({func_name})-{gate_id}-{request_id}",
                func_args=(gate_id, request_id, func_name, args, kwargs)
            )
        else:
            self.backend_tasks.add(
                task := self._loop.create_task(
                    self.request_backend(
                        service_name, gate_id,
                        request_id, body
                    )
                )
            )
            task.add_done_callback(self.cleanup_backend_task)

    async def process_gate_reply(
        self, request_id, service_name, body
    ):
        if client_id := self.request_map.get(request_id, None):
            if client_id in self.clients:
                self.frontend_tasks.add(
                    task := self._loop.create_task(
                        self.reply_frontend(
                            client_id, service_name,
                            request_id, *body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_frontend_task)
            elif gate := self.gate_cluster.get_workers().get(client_id):
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.reply_gate(
                            gate, service_name,
                            request_id, *body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)

        elif request_id in self.gate_replies:
            if len(body) == 1:
                body = msg_unpack(body[0])
                self.gate_replies[request_id].set_result(body)
            elif len(body) > 1:
                *option, body = body
                if option[0] == Settings.STREAM_GENERATOR_TAG:
                    if not (reply := self.gate_replies[request_id]).done():
                        stream_reply = StreamReply(
                            Settings.STREAM_END_TAG,
                            maxsize=Settings.STREAM_REPLY_MAXSIZE,
                            timeout=Settings.REPLY_TIMEOUT
                        )
                        self.gate_replies[request_id].set_result(
                            stream_reply
                        )
                    else:
                        stream_reply = reply.result()
                    if (len(option) > 1 and option[1] ==
                            Settings.STREAM_EXCEPT_TAG):
                        exc = RemoteException(msg_unpack(body))
                        await throw_exception_agenerator(
                            stream_reply, exc
                        )
                    else:
                        if body != Settings.STREAM_END_TAG:
                            body = msg_unpack(body)
                        await stream_reply.asend(body)
                elif option[0] == Settings.STREAM_HUGE_DATA_TAG:
                    if not (reply := self.gate_replies[request_id]).done():
                        huge_reply = HugeData(
                            Settings.HUGE_DATA_END_TAG,
                            Settings.HUGE_DATA_EXCEPT_TAG,
                            compress_module=(
                                Settings.HUGE_DATA_COMPRESS_MODULE
                            ),
                            compress_level=(
                                Settings.HUGE_DATA_COMPRESS_LEVEL
                            ),
                            blksize=Settings.HUGE_DATA_SIZEOF
                        )
                        self.gate_replies[request_id].set_result(huge_reply)
                    else:
                        huge_reply = reply.result()
                    await self._loop.run_in_executor(
                        self.thread_executor, huge_reply.add_data, body
                    )

    async def process_whisper_command(self, gate: RemoteGate, messages):
        if not gate:
            return
        if not gate.ready:
            return await self.now_ban_gate(gate)
        await self.delay_ban_gate(gate)
        try:
            (
                service_name,
                request_id,
                *body
            ) = messages
        except MsgUnpackError:
            return
        if request_id in self.request_map or request_id in self.gate_replies:
            await self.process_gate_reply(request_id, service_name, body)
        else:
            await self.process_gate_request(
                gate.identity, request_id, service_name, body
            )

    async def send_notice_command(
        self, notice_id, event_name, body
    ):
        """
        只发不管
        """
        if Settings.DEBUG > 1:
            Glogger.debug(f"Send NOTICE ({event_name})")
        option = (notice_id, event_name)
        for gate in self.gate_cluster.get_workers().values():
            await self.send_to_gate(
                gate,
                Settings.GATE_COMMAND_NOTICE,
                option,
                body
            )

    async def process_notice_command(self, gate: RemoteGate, messages):
        """
        只收不回
        """
        if not gate:
            return
        if not gate.ready:
            return await self.now_ban_gate(gate)
        await self.delay_ban_gate(gate)
        try:
            (
                notice_id,
                event_name,
                *body
            ) = messages
            event_name = from_bytes(event_name)
            if event_name not in self.event_handlers:
                return
        except ValueError:
            return
        if Settings.DEBUG > 1:
            Glogger.debug(f"notice_id: {notice_id}, event_name: {event_name}")
        self.gate_tasks.add(
            task := self._loop.create_task(
                self.event_handlers[event_name](gate, *body),
                name=f"{event_name}-{notice_id}"
            )
        )
        task.add_done_callback(self.cleanup_gate_task)

    async def process_messages_from_gate(self, messages):
        try:
            (
                gate_id,
                empty_frame,
                gate_version,
                command,
                *messages
            ) = messages
            if (
                    empty_frame
                    or gate_version != Settings.GATE_MEMBER
                    or command not in (
                        Settings.GATE_COMMAND_NOTICE,
                        Settings.GATE_COMMAND_INVITE,
                        Settings.GATE_COMMAND_JOIN,
                        Settings.GATE_COMMAND_WHISPER,
                        Settings.GATE_COMMAND_HEARTBEAT,
                        Settings.GATE_COMMAND_LEAVE
                    )
            ):
                return
        except ValueError:
            return

        if Settings.DEBUG > 1:
            Glogger.debug(
                f"gate id: {gate_id}\n"
                f"command: {command}\n"
                f"messages: {messages}\n"
            )

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
                request_id = b"gate_" + command
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    if Settings.DEBUG:
                        Glogger.warning(
                            f"ZAP verification of Gate({gate_id})"
                            f" fails,'{mechanism}' mechanism is used, and the "
                            f"credential is '{credentials}', and the request_id "
                            f"is '{request_id}'"
                        )
                    return
            else:
                return

        if not (gate := self.gate_cluster.get_workers().get(gate_id, None)):
            Glogger.warning(f"The gate({gate_id}) was not registered.")

        if command == Settings.GATE_COMMAND_INVITE:
            await self.process_invite_command(gate_id)

        elif command == Settings.GATE_COMMAND_JOIN:
            if gate:
                Glogger.warning(
                    f"recv JOIN command, but Gate({gate_id}) already join."
                )
                return await self.now_ban_gate(gate)
            try:
                active_services = msg_unpack(messages[0])
            except MsgUnpackError:
                return
            await self.process_join_command(gate_id, active_services)

        elif command == Settings.GATE_COMMAND_HEARTBEAT:
            if not messages and gate:
                if not gate.ready:
                    return await self.now_ban_gate(gate)
                await self.delay_ban_gate(gate)

        elif command == Settings.GATE_COMMAND_WHISPER:
            await self.process_whisper_command(gate, messages)

        elif command == Settings.GATE_COMMAND_NOTICE:
            await self.process_notice_command(gate, messages)

        elif command == Settings.GATE_COMMAND_LEAVE:
            if gate:
                gate.ready = False
                gate.expiry = self._loop.time()
                await self.now_ban_gate(gate)

    async def reply_gate(
        self,
        gate: RemoteGate,
        service: str,
        request_id: bytes,
        *body,
    ):
        await self.send_to_gate(
            gate,
            Settings.GATE_COMMAND_WHISPER,
            (service, request_id),
            body
        )

    async def send_to_gate(
        self,
        gate: Union[RemoteGate, z_aio.Socket],
        command: Union[bytes, str],
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[Union[bytes, str], tuple[Union[bytes, str], ...]] = None,
    ):
        if isinstance(messages, (bytes, str)):
            messages = (messages,)
        elif not messages:
            messages = tuple()
        if option:
            messages = (*option, *messages)
        messages = (
            b"",
            Settings.GATE_MEMBER,
            command,
            *(await self.build_gate_zap_frames()),
            *messages
        )
        messages = tuple(to_bytes(s) for s in messages)
        if isinstance(gate, RemoteGate):
            await gate.sock.send_multipart(messages)
            # if gate := self.gate_cluster.get_workers().get(gate.identity):
            gate.last_message_sent_time = self._loop.time()
        elif isinstance(gate, z_aio.Socket):
            await gate.send_multipart(messages)

    def _multicast_recv_callback(self, data, addr):
        task = self._loop.create_task(
            self.process_mcast_messages(data, addr)
        )
        self.gate_tasks.add(task)
        task.add_done_callback(self.cleanup_gate_task)

    async def _multicast_self(self):
        if Settings.DEBUG:
            Glogger.debug("Multicast.")
        mcast_group, mcast_port = (
            Settings.GATE_MULTICAST_GROUP, Settings.GATE_MULTICAST_PORT
        )
        if Settings.GATE_IP_VERSION == 6:
            raw_sock = socket.socket(
                socket.AF_INET6, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            raw_sock.setsockopt(
                socket.IPPROTO_IPV6,
                socket.IPV6_MULTICAST_HOPS,
                Settings.GATE_MULTICAST_HOP_LIMIT
            )
            mreq = struct.pack(
                "16sI",
                socket.inet_pton(socket.AF_INET6, mcast_group),
                socket.INADDR_ANY
            )
            raw_sock.setsockopt(
                socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq
            )
        else:
            mcast_port = int(mcast_port)
            raw_sock = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            raw_sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_TTL, Settings.GATE_MULTICAST_TTL
            )
            mreq = struct.pack(
                "4sL", socket.inet_aton(mcast_group), socket.INADDR_ANY
            )
            raw_sock.setsockopt(
                socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq
            )
        raw_sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEPORT, 1
        )
        raw_sock.bind(('', mcast_port))
        self.mcast_sock, _ = await self._loop.create_datagram_endpoint(
            lambda: MulticastProtocol(self._multicast_recv_callback),
            sock=raw_sock
        )

        if Settings.DEBUG > 1:
            Glogger.debug(f"send mcast messages:\n {self.mcast_msg}")
        while 1:
            await asyncio.sleep(1e-3 * self.heartbeat)
            try:
                await self._loop.run_in_executor(
                    self.thread_executor,
                    self.mcast_sock.sendto,
                    self.mcast_msg, (mcast_group, mcast_port)
                )
                # self.mcast_sock.send(msg)
            except Exception as e:
                if Settings.GATE_IP_VERSION == 6:
                    raw_sock.setsockopt(
                        socket.IPPROTO_IPV6,
                        socket.IPV6_LEAVE_GROUP, mreq
                    )
                else:
                    raw_sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_DROP_MEMBERSHIP, mreq
                    )
                self.mcast_sock.close()
                self.mcast_sock = None
                raise e

    async def process_mcast_messages(self, messages, addr: tuple):
        try:
            if Settings.DEBUG > 2:
                Glogger.debug(
                    f"recv mcast message from {addr}:\n{messages}"
                )
            v_end = 16 + len(Settings.GATE_MEMBER)
            c_end = v_end + len(Settings.GATE_COMMAND_NOTICE)
            gate_id, gate_v, command, cluster_id, port = (
                messages[:16],
                messages[16:v_end],
                messages[v_end:c_end],
                messages[-20:-4],
                from_bytes(messages[-4:], to_num=True),
            )
        except (ValueError, TypeError):
            return
        if (
                gate_id == self.identity
                or gate_v != Settings.GATE_MEMBER
                or command != Settings.GATE_COMMAND_NOTICE
                or cluster_id != self.gate_cluster.identity
        ):
            return
        if Settings.DEBUG > 1:
            Glogger.debug(
                f"gate_id: {gate_id}, gate_v: {gate_v}, command: {command}, "
                f"cluster_id: {cluster_id}, port: {port}"
            )
        if gate := self.gate_cluster.get_workers().get(gate_id, None):
            await self.delay_ban_gate(gate)
        elif gate_sock := self.gate_cluster.unready_gates.get(gate_id):
            await self.send_invite_command(gate_sock)
        elif gate_id not in self.gate_cluster.online_nodes():
            host = addr[0]
            if len(addr) > 2:
                host = f"[{host}]"
            self.gate_tasks.add(
                task := self._loop.create_task(
                    self.connect_gate(gate_id, f"tcp://{host}:{port}")
                )
            )
            task.add_done_callback(self.cleanup_gate_task)

    async def _broker_loop(self):
        try:
            if self.frontend:
                self._poller.register(self.frontend, z_const.POLLIN)
            if self.backend:
                self._poller.register(self.backend, z_const.POLLIN)
            if self.gate:
                self._poller.register(self.gate, z_const.POLLIN)
            if self.zap:
                self._poller.register(self.zap, z_const.POLLIN)
            self.running = True

            while 1:
                socks = await self._poller.poll(self.heartbeat)
                socks = dict(socks)
                if self.backend and self.backend in socks:
                    replies = await self.backend.recv_multipart()
                    bt = self._loop.create_task(
                        self.process_messages_from_backend(replies),
                    )
                    self.backend_tasks.add(bt)
                    bt.add_done_callback(self.cleanup_backend_task)

                if self.frontend and self.frontend in socks:
                    replies = await self.frontend.recv_multipart()
                    # 同后端任务处理。
                    ft = self._loop.create_task(
                        self.process_messages_from_frontend(replies),
                    )
                    self.frontend_tasks.add(ft)
                    ft.add_done_callback(self.cleanup_frontend_task)

                if self.gate and self.gate in socks:
                    messages = await self.gate.recv_multipart()
                    gt = self._loop.create_task(
                        self.process_messages_from_gate(messages)
                    )
                    self.gate_tasks.add(gt)
                    gt.add_done_callback(self.cleanup_gate_task)

                if self.zap and self.zap in socks:
                    replies = await self.zap.recv_multipart()
                    zt = self._loop.create_task(
                        self.process_replies_from_zap(replies),
                    )
                    self.zap_tasks.add(zt)
                    zt.add_done_callback(self.zap_tasks.remove)
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Glogger.error(exception)
            raise
        finally:
            await self.disconnect_all()
            Glogger.info("Gate loop exit.")
            self.close()

    @interface
    def query_service(self, service_name, **kwargs):
        if service_name in self.services:
            return self.services[service_name].description
        elif service_name in self.gate_cluster.services:
            return f"GateCluster.{service_name}"
        else:
            return None

    @interface
    def get_services(self, **kwargs):
        services = list(self.services.keys())
        services.append(Settings.MDP_INTERNAL_SERVICE)
        services.extend(self.gate_cluster.services.keys())
        return services

    async def update_service(self, gate: RemoteGate, *body):
        service_name = msg_unpack(body[0])
        num = msg_unpack(body[1])
        assert isinstance(service_name, str)
        assert isinstance(num, int)
        if Settings.DEBUG > 1:
            Glogger.debug(f"update active service: {service_name}({num})")
        gate.active_services[service_name] = num
        if service_name not in self.gate_cluster.services:
            self.gate_cluster.services[service_name] = asyncio.Queue()
            self.gate_cluster.services[service_name].put_nowait(gate.identity)


class Client:

    def __init__(
        self,
        broker_addr: str = None,
        identity: str = None,
        context: Context = None,
        heartbeat: int = None,
        reply_timeout: float = None,
        zap_mechanism: Union[str, bytes] = None,
        zap_credentials: tuple = None,
    ):
        """
        客户端，
        TODO: 对于没有收到回复的请求记录并保存下来，可以设置重试次数来重新请求。
        :param context: zmq 的上下文实例，为空则创建。
        :param heartbeat: 维持连接的心跳消息间隔，默认为 MDP_HEARTBEAT，单位是毫秒。
        :param reply_timeout: 等待回复的超时时间
        :param zap_mechanism: zap 的验证机制。
        :param zap_credentials: zap 的凭据帧列表，不填则为空列表。
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        if not identity:
            identity = f"gc-{secrets.token_hex(8)}"
        self.identity = name2uuid(identity).bytes
        if Settings.DEBUG > 1:
            Clogger.debug(f"Client id: {self.identity}")
        self.ready: Optional[asyncio.Future] = None
        self._broker_addr = broker_addr
        self._recv_task: Optional[asyncio.Task] = None
        zap_mechanism, zap_credentials = check_zap_args(
            zap_mechanism, zap_credentials
        )
        if zap_mechanism:
            self.zap_frames = (zap_mechanism, *zap_credentials)
        else:
            self.zap_frames = None

        if not context:
            self.ctx = Context()
        else:
            self.ctx = context
        set_ctx(self.ctx)
        self.socket: Optional[z_aio.Socket] = None
        self._poller = z_aio.Poller()

        if not heartbeat:
            heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat = heartbeat
        self.heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.last_message_sent_time: float = 0.0

        if not reply_timeout:
            reply_timeout = Settings.REPLY_TIMEOUT
        self.reply_timeout = reply_timeout

        self._executor = ThreadPoolExecutor()
        self.replies: dict[bytes, asyncio.Future] = dict()
        self._process_replies_tasks = set()
        self._clear_replies_tasks = set()
        self.stream_reply_maxsize = Settings.STREAM_REPLY_MAXSIZE

        self.default_service = Settings.SERVICE_DEFAULT_NAME
        self._remote_services: list[str] = []
        self._remote_service = self._remote_func = None

    async def connect(self, broker_addr=None):
        if self._recv_task is not None:
            raise RuntimeError("Broker is connected.")
        if broker_addr:
            self._broker_addr = broker_addr
        self._broker_addr = check_socket_addr(self._broker_addr)
        Clogger.info(
            f"Client is attempting to connet to the broker at "
            f"{self._broker_addr}."
        )
        self.socket = self.ctx.socket(
            z_const.DEALER, z_aio.Socket
        )
        set_sock(self.socket, {z_const.IDENTITY: self.identity})
        self.socket.connect(self._broker_addr)
        self._poller.register(self.socket, z_const.POLLIN)
        self.ready = self._loop.create_future()
        self._recv_task = self._loop.create_task(self._recv())
        await self._get_services()

    async def disconnect(self):
        if self.socket is not None:
            self._poller.unregister(self.socket)
            self.socket.disconnect(self._broker_addr)
            self.socket.close()
            self.socket = None

    def close(self):
        if self._recv_task and not self._recv_task.done():
            Clogger.warning("Client close.")
            self._recv_task.cancel()
            self._recv_task = None
        if self._executor:
            self._executor.shutdown(False)
            self._executor = None
        self.ready = None

    async def _build_zap_frames(self):
        if self.zap_frames:
            return tuple(to_bytes(f) for f in self.zap_frames)
        else:
            return tuple()

    async def _send_to_majordomo(
        self, service_name: Union[str, bytes], *body,
    ) -> bytes:
        """
        客户端使用 DEALER 类型 socket，每个请求都有一个id，
        worker 每个回复都会带上这个请求id。 客户端需要将回复和请求对应上。
        :param service_name: 要访问的服务的名字
        """
        request_id = uuid4().bytes
        request = (
            b"",
            Settings.MDP_CLIENT,
            service_name,
            *(await self._build_zap_frames()),
            request_id,
            *body
        )
        request = tuple(to_bytes(frame) for frame in request)
        await self.socket.send_multipart(request)
        self.last_message_sent_time = self._loop.time()
        return request_id

    async def _clear_reply(self, request_id):
        if request_id in self.replies:
            reply = self.replies[request_id].result()
            if isinstance(reply, StreamReply):
                if reply.is_exit():
                    self.replies.pop(request_id).done()
                else:
                    t = self._loop.create_task(
                        self._clear_reply(request_id)
                    )
                    self._clear_replies_tasks.add(t)
                    t.add_done_callback(self._clear_replies_tasks.remove)
            else:
                self.replies.pop(request_id).done()

    async def _request(
        self, service_name: Union[str, bytes],
        func_name: Union[str, bytes], args: tuple, kwargs: dict,
    ):
        f = self._loop.create_future()
        if Settings.DEBUG > 2:
            f.start_time = self._loop.time()
        request_id = await self._send_to_majordomo(
            service_name,
            msg_pack(func_name), msg_pack(args), msg_pack(kwargs)
        )
        self.replies[request_id] = f
        try:
            response = await asyncio.wait_for(
                self.replies[request_id],
                self.reply_timeout
            )
            if Settings.DEBUG > 2:
                Clogger.debug(
                    f"request(request_id) use time from majordomo:"
                    f" {self._loop.time() - f.start_time}"
                )
            if isinstance(response, StreamReply):
                return response
            elif isinstance(response, HugeData):
                result = b""
                executor = ProcessPoolExecutor()
                async for data in response.decompress(
                        self._loop, Settings.HUGE_DATA_SIZEOF
                ):
                    result += data
                try:
                    result = await run_in_executor(
                        self._loop, executor, msg_unpack, result
                    )
                    return result
                finally:
                    response.destroy()
                    executor.shutdown(False, cancel_futures=True)
            else:
                if exception := response.get("exception"):
                    raise RemoteException(exception)
                return response.get("result")
        except asyncio.TimeoutError:
            raise TimeoutError(
                "Timeout while requesting the Majordomo service."
            )
        except Exception as e:
            raise e
        finally:
            self._clear_replies_tasks.add(
                t := self._loop.create_task(self._clear_reply(request_id))
            )
            t.add_done_callback(self._clear_replies_tasks.remove)

    def _cleanup_process_tasks(self, task: asyncio.Task):
        self._process_replies_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Clogger.error(f"process reply task exception:\n{exception_info}")

    async def _process_reply(self, messages):
        (
            service_name,
            request_id,
            *body
        ) = messages
        if Settings.DEBUG > 1:
            Clogger.debug(
                f"service_name: {service_name}\n"
                f"request_id: {request_id}"
            )
        if request_id not in self.replies:
            return
        if len(body) == 1:
            body = msg_unpack(body[0])
            if Settings.DEBUG > 1:
                Clogger.debug(f"body: {body}")
            self.replies[request_id].set_result(body)
        elif len(body) > 1:
            *option, body = body
            if Settings.DEBUG > 1:
                Clogger.debug(f"option: {option}")
            if option[0] == Settings.STREAM_GENERATOR_TAG:
                if not (reply := self.replies[request_id]).done():
                    stream_reply = StreamReply(
                        Settings.STREAM_END_TAG,
                        maxsize=self.stream_reply_maxsize,
                        timeout=self.reply_timeout
                    )
                    self.replies[request_id].set_result(stream_reply)
                else:
                    stream_reply = reply.result()
                if (len(option) > 1 and option[1] ==
                        Settings.STREAM_EXCEPT_TAG):
                    exc = RemoteException(msg_unpack(body))
                    await throw_exception_agenerator(
                        stream_reply, exc
                    )
                else:
                    if body != Settings.STREAM_END_TAG:
                        body = msg_unpack(body)
                    await stream_reply.asend(body)
            elif option[0] == Settings.STREAM_HUGE_DATA_TAG:
                if not (reply := self.replies[request_id]).done():
                    huge_reply = HugeData(
                        Settings.HUGE_DATA_END_TAG,
                        Settings.HUGE_DATA_EXCEPT_TAG,
                        compress_module=(
                            Settings.HUGE_DATA_COMPRESS_MODULE
                        ),
                        compress_level=(
                            Settings.HUGE_DATA_COMPRESS_LEVEL
                        ),
                        blksize=Settings.HUGE_DATA_SIZEOF,
                        timeout=Settings.TIMEOUT
                    )
                    self.replies[request_id].set_result(huge_reply)
                else:
                    huge_reply = reply.result()
                if len(option) > 1 and option[1] == Settings.STREAM_EXCEPT_TAG:
                    huge_reply._remote_exception = body
                else:
                    huge_reply.add_data(body)

    async def _majordomo_service(
        self, func_name: str, *args, **kwargs
    ):
        return await self._request(
            Settings.MDP_INTERNAL_SERVICE, func_name, args, kwargs
        )

    async def _send_heartbeat(self):
        while 1:
            time_from_last_message = (
                    self._loop.time() - self.last_message_sent_time
            )
            if time_from_last_message >= 1e-3 * self.heartbeat:
                await self._majordomo_service("keepalive", b"heartbeat")
                await asyncio.sleep(1e-3 * self.heartbeat)
            else:
                await asyncio.sleep(
                    1e-3 * self.heartbeat - time_from_last_message
                )

    async def _get_services(self):
        services = await self._majordomo_service("get_services")
        self._remote_services = services
        if not self.ready.done():
            self.ready.set_result(True)

    async def _query_service(self, service_name: str):
        return await self._majordomo_service(
            "query_service", msg_pack(service_name)
        )

    async def _recv(self):
        try:
            majordomo_liveness = self.heartbeat_liveness
            self._heartbeat_task = self._loop.create_task(
                self._send_heartbeat()
            )
            while 1:
                socks = await self._poller.poll(self.heartbeat)
                if not socks:
                    majordomo_liveness -= 1
                    if majordomo_liveness == 0:
                        Clogger.error("Majordomo offline")
                        break
                    continue
                messages = await self.socket.recv_multipart()
                try:
                    (
                        empty_frame,
                        mdp_client_version,
                        *messages
                    ) = messages
                    if empty_frame:
                        continue
                except ValueError:
                    continue
                if mdp_client_version != Settings.MDP_CLIENT:
                    continue
                majordomo_liveness = self.heartbeat_liveness
                self._process_replies_tasks.add(
                    t := self._loop.create_task(
                        self._process_reply(messages)
                    )
                )
                t.add_done_callback(self._cleanup_process_tasks)
        except asyncio.CancelledError:
            Clogger.warning("recv task cancelled.")
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Clogger.error(exception)
            raise
        finally:
            self._remote_services.clear()
            await self.disconnect()
            self._heartbeat_task.cancel()
            self.ready = None

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
        if not self._recv_task:
            await self.connect()
        if not self.ready:
            raise RuntimeError("Gate service not yet ready.")
        try:
            await asyncio.wait_for(self.ready, self.reply_timeout)
        except asyncio.TimeoutError:
            raise RuntimeError("waiting for Gate service timed out")
        if not service_name:
            service_name = self.default_service
        if service_name not in self._remote_services:
            raise AttributeError(
                f"The remote service named \"{service_name}\" was not "
                f"found."
            )
        if (replies_len := len(self.replies)) > Settings.MESSAGE_MAX:
            msg = (
                f"The number({replies_len}) of pending requests has exceeded "
                f"the cache limit for the requesting task. The limit is "
                f"{Settings.MESSAGE_MAX} requests."
            )
            if Settings.SECURE:
                raise RuntimeError(msg)
            Clogger.warning(msg)
        return await self._request(service_name, func_name, args, kwargs)

    def __getattr__(self, item):
        if not self._remote_service:
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
        try:
            return self.knock(
                self._remote_service, self._remote_func, *args, **kwargs
            )
        finally:
            self._remote_service = self._remote_func = None


class SimpleClient:
    """
    简单客户端，只适合用于测试和调用返回结果是单个小型数据的远程方法
    """
    def __init__(
        self,
        broker_addr,
        reply_timeout: float = None,
        zap_mechanism: Union[str, bytes] = None,
        zap_credentials: tuple = None,
    ):
        self.identity = name2uuid(f"sgc-{secrets.token_hex(8)}").bytes
        if Settings.DEBUG > 1:
            Clogger.debug(f"SimpleClient id: {self.identity}")
        self._broker_addr = check_socket_addr(broker_addr)
        zap_mechanism, zap_credentials = check_zap_args(
            zap_mechanism, zap_credentials
        )
        if zap_mechanism:
            self.zap_frames = (zap_mechanism, *zap_credentials)
        else:
            self.zap_frames = None
        self.ctx = SyncContext()
        set_ctx(self.ctx)
        if not reply_timeout:
            reply_timeout = Settings.REPLY_TIMEOUT
        self.reply_timeout = reply_timeout
        self.socket = self.ctx.socket(z_const.REQ, SyncSocket)
        set_sock(self.socket, {z_const.IDENTITY: self.identity})
        self.socket.connect(self._broker_addr)
        self._remote_service = self._remote_func = None

    def _build_zap_frames(self):
        if self.zap_frames:
            return tuple(to_bytes(f) for f in self.zap_frames)
        else:
            return tuple()

    def _close(self):
        self.socket.disconnect(self._broker_addr)
        self.socket.close()

    def __del__(self):
        self._close()

    def __getattr__(self, item):
        if not self._remote_service:
            self._remote_service = self._remote_func
        else:
            self._remote_service = ".".join(
                (self._remote_service, self._remote_func)
            )
        self._remote_func = item
        return self

    def __call__(self, *args, **kwargs):
        """
        执行rpc调用
        """
        request_id = uuid4().bytes
        request = (
            Settings.MDP_CLIENT,
            self._remote_service if self._remote_service else 
            Settings.SERVICE_DEFAULT_NAME,
            *(self._build_zap_frames()),
            request_id,
            msg_pack(self._remote_func),
            msg_pack(args),
            msg_pack(kwargs)
        )
        request = tuple(to_bytes(frame) for frame in request)
        try:
            self.socket.send_multipart(request)
            response = self.socket.recv_multipart()
            (
                mdp_client_version,
                service_name,
                _request_id,
                *body
            ) = response
            if _request_id != request_id:
                raise RuntimeError("request_id inconsistent")
            if len(body) == 1:
                return msg_unpack(body[0])
            elif len(body) > 1:
                raise RuntimeError("Does not support streaming data.")
        finally:
            self._remote_service = self._remote_func = None
