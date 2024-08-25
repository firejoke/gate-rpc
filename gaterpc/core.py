# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/3/30 11:09
import asyncio
import inspect
import sys
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Coroutine, Callable, AsyncGenerator, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from traceback import format_exception, format_tb
from typing import Annotated, Any, List, Tuple, Union, overload, Optional
from uuid import uuid4
from logging import getLogger

import zmq.constants as z_const
import zmq.asyncio as z_aio
import zmq.error as z_error
from gaterpc.exceptions import HugeDataException
from pkg_resources import resource_listdir
from zmq.auth import Authenticator

from .global_settings import Settings
from .exceptions import (
    BusyGateError, BusyWorkerError,
    GateUnAvailableError, ServiceUnAvailableError,
    RemoteException,
)
from .utils import (
    HugeData, LRUCache, MsgUnpackError, StreamReply,
    check_socket_addr, generator_to_agenerator, throw_exception_agenerator,
    interface, run_in_executor, from_bytes, msg_pack, msg_unpack, to_bytes,
)


__all__ = ["Worker", "Service", "AsyncZAPService", "AMajordomo", "Client"]

logger = getLogger("gaterpc")
Wlogger = getLogger("gaterpc.worker")
Mlogger = getLogger("gaterpc.majordomo")
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
            ctx.set(op, v)
        except AttributeError:
            pass


def set_sock(sock: z_aio.Socket, options: dict = None):
    _options = Settings.ZMQ_CONTEXT
    if options:
        _options.update(options)
    for op, v in _options.items():
        try:
            if op == z_const.HWM and isinstance(v, int):
                sock.set_hwm(v)
            else:
                sock.set(op, v)
        except AttributeError:
            pass


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
    async def acquire_idle_worker(self) -> Optional[ABCWorker]:
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
        socket: z_aio.Socket,
        mdp_version: bytes,
        service: "ABCService"
    ) -> None:
        """
        远程 worker 的本地映射
        :param identity: 远程 worker 的id
        :param heartbeat: 和远程 worker 的心跳间隔，单位是毫秒
        :param max_allowed_request: 能处理的最大请求数
        :param socket: socket
        :param mdp_version: MDP版本
        :param service: 该 worker 提供的服务
        """
        self.identity = identity
        self.socket = socket
        self.mdp_version = mdp_version
        self.ready = False
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
        identity: str = "gw",
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

        self.identity = f"{identity}-{uuid4().hex}".encode("utf-8")
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
        set_ctx(self._ctx)
        self._socket = None
        self._poller = z_aio.Poller()
        if not heartbeat:
            heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat = heartbeat
        self.heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS
        self.expiry = self.heartbeat_liveness * self.heartbeat

        self.sent_tasks = set()
        self.last_message_sent_time: float = heartbeat

        if max_allowed_request:
            self.max_allowed_request = max_allowed_request
        else:
            self.max_allowed_request = Settings.MESSAGE_MAX
        self.requests = set()

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
        self._heartbeat_task: Optional[asyncio.Task] = None

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
            self._socket.set(z_const.IDENTITY, self.identity)
            set_sock(self._socket)
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
            Wlogger.warning("Disconnecting from broker")
            # await self.send_to_majordomo(
            #     Settings.MDP_COMMAND_DISCONNECT
            # )
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

    async def send_to_majordomo(
        self,
        command: bytes,
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[Union[bytes, str], tuple[Union[bytes, str], ...]] = None
    ):
        """
        TODO: 在MDP的Worker端发出的消息的命令帧后面加一个worker id帧，
            用作掉线重连，幂等。
        """
        if not self._socket:
            return
        reply = [
            b"",
            Settings.MDP_WORKER,
            command,
        ]
        if self.zap_frames:
            reply.extend(self.zap_frames)
        if option:
            reply.extend(option)
        if isinstance(messages, (tuple, list)):
            reply.extend(messages)
        elif isinstance(messages, (bytes, str)):
            reply.append(messages)
        reply = tuple(to_bytes(s) for s in reply)
        await self._socket.send_multipart(reply)
        self.last_message_sent_time = self._loop.time()

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

    async def process_request(
        self, client_id: bytes, request_id: bytes,
        func_name: str, args: tuple, kwargs: dict
    ):
        result = exception = None
        try:
            if len(self.requests) > self.max_allowed_request:
                if not Settings.SECURE:
                    return
                raise RuntimeError("Request count exceeds the maximum value")
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
            task = self._loop.create_task(
                self.send_hugedata(option, result),
                name=f"SendHD-{client_id}-{request_id}"
            )
        elif isinstance(result, AsyncGenerator):
            task = self._loop.create_task(
                self.send_agenerator(option, result),
                name=f"SendAG-{client_id}-{request_id}"
            )
        else:
            reply = msg_pack(
                await generate_reply(result=result, exception=exception)
            )
            task = self._loop.create_task(
                self.send_to_majordomo(
                    Settings.MDP_COMMAND_REPLY, option, reply
                )
            )
        self.sent_tasks.add(task)
        task.add_done_callback(self.cleanup_sent)

    async def send_heartbeat(self):
        while 1:
            time_from_last_message = (
                    self._loop.time() - self.last_message_sent_time
            )
            if time_from_last_message >= 1e-3 * self.heartbeat:
                await self.send_to_majordomo(
                    Settings.MDP_COMMAND_HEARTBEAT,
                )
                await asyncio.sleep(1e-3 * self.heartbeat)
            else:
                await asyncio.sleep(
                    1e-3 * self.heartbeat - time_from_last_message
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
                if not self._heartbeat_task:
                    self._heartbeat_task = self._loop.create_task(
                        self.send_heartbeat()
                    )
                messages = await self._socket.recv_multipart()
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
                # Wlogger.debug(
                #     f"worker recv\n"
                #     f"command: {command}\n"
                #     f"messages: {messages}"
                # )
                majordomo_liveness = self.heartbeat_liveness
                if command == Settings.MDP_COMMAND_HEARTBEAT:
                    pass
                elif command == Settings.MDP_COMMAND_DISCONNECT:
                    Wlogger.warning(f"{self.identity} disconnected")
                    break
                elif command == Settings.MDP_COMMAND_REQUEST:
                    try:
                        (
                            client_id,
                            empty,
                            request_id,
                            func_name,
                            *params
                        ) = messages
                        if empty:
                            continue
                        if not params:
                            args, kwargs = tuple(), dict()
                        elif len(params) == 1:
                            args, kwargs = msg_unpack(params[0]), dict()
                        elif len(params) == 2:
                            args, kwargs = (msg_unpack(p) for p in params)
                        else:
                            continue
                        func_name = msg_unpack(func_name)
                    except MsgUnpackError:
                        continue
                    task = self._loop.create_task(
                        self.process_request(
                            client_id, request_id,
                            func_name, args, kwargs
                        ),
                        name=f"Func({func_name})-{client_id}-{request_id}"
                    )
                    self.requests.add(task)
                    task.add_done_callback(self.cleanup_request)
        except asyncio.CancelledError:
            self._reconnect = False
            pass
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Wlogger.error(exception)
            raise
        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
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
        self.workers: dict[bytes, ABCWorker] = dict()
        self.idle_workers: asyncio.Queue[bytes] = asyncio.Queue()
        self.running = False

    def add_worker(self, worker: ABCWorker):
        if worker.identity in self.workers:
            return
        worker.service = self
        self.workers[worker.identity] = worker
        self.idle_workers.put_nowait(worker.identity)
        self.running = True

    def remove_worker(self, worker: ABCWorker):
        if worker.identity in self.workers:
            self.workers.pop(worker.identity)
        if hasattr(worker, "stop"):
            worker.stop()
        if not self.workers:
            self.running = False

    def get_workers(self) -> dict[bytes, ABCWorker]:
        return self.workers

    async def acquire_idle_worker(self) -> Optional[ABCWorker]:
        while 1:
            try:
                worker_id = await asyncio.wait_for(
                    self.idle_workers.get(),
                    Settings.TIMEOUT
                )
            except TimeoutError:
                return None
            try:
                worker = self.workers[worker_id]
                if worker.is_alive() and worker.max_allowed_request != 0:
                    worker.max_allowed_request -= 1
                    if worker.max_allowed_request > 0:
                        await self.idle_workers.put(worker.identity)
                    return worker
            except KeyError:
                continue

    async def release_worker(self, worker: ABCWorker):
        if worker.identity in self.workers and worker.is_alive():
            if worker.max_allowed_request == 0:
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
            identity=f"{self.name}_Worker",
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
        # self.zap_socket.linger = 1
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

        # self.log.debug(
        #     f"request address: {request_address}, "
        #     f"version: {version}, request_id: {request_id}, "
        #     f"domain: {domain}, address: {address}, "
        #     f"identity: {identity}, mechanism: {mechanism}"
        # )

        # Is address is explicitly allowed or _denied?
        allowed = False
        denied = False
        reason = b"NO ACCESS"

        if self._allowed:
            if address in self._allowed:
                allowed = True
                # self.log.debug("PASSED (allowed) address=%s", address)
            else:
                denied = True
                reason = b"Address not allowed"
                # self.log.debug("DENIED (not allowed) address=%s", address)

        elif self._denied:
            if address in self._denied:
                denied = True
                reason = b"Address denied"
                # self.log.debug("DENIED (denied) address=%s", address)
            else:
                allowed = True
                # self.log.debug("PASSED (not denied) address=%s", address)

        # Perform authentication mechanism-specific checks if necessary
        username = "anonymous"
        if not denied:
            if mechanism == b'NULL' and not allowed:
                # For NULL, we allow if the address wasn't denied
                # self.log.debug("ALLOWED (NULL)")
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

            # if allowed:
            #     self.log.debug(
            #         "ALLOWED (PLAIN) domain=%s username=%s password=%s",
            #         domain,
            #         username,
            #         password,
            #     )
            # else:
            #     self.log.debug("DENIED %s", reason)

        else:
            reason = b"No passwords defined"
            # self.log.debug("DENIED (PLAIN) %s", reason)

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
        # self.log.debug(
        #     f"ZAP reply user_id={user_id} request_id={request_id}"
        #     f" code={status_code} text={status_text}"
        # )
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


class RemoteGateRPC(RemoteWorker):

    def __init__(
        self,
        identity: bytes,
        heartbeat: int,
        socket: z_aio.Socket,
        mdp_version: bytes,
        service: "GateClusterService",
        work_services: list[str]
    ):
        """
        远程 GateRPC 的本地映射
        :param identity:
        :param heartbeat:
        :param service:
        """
        super().__init__(
            identity, heartbeat,
            Settings.MESSAGE_MAX, socket, mdp_version,
            service
        )
        self.work_services = work_services
        self.addr = None


class GateClusterService(ABCService):

    def __init__(self, local_gate: "AMajordomo"):
        self.local_gate = local_gate
        self.name = Settings.GATE_CLUSTER_NAME
        self.description = Settings.GATE_CLUSTER_DESCRIPTION
        self.services: dict[str, deque[bytes]] = dict()
        self.workers: dict[bytes, RemoteGateRPC] = dict()
        self.idle_workers: deque[bytes] = deque()
        self.unready_gates: dict[bytes, str] = dict()

    def add_worker(self, gate: Optional[RemoteGateRPC]) -> None:
        if gate.identity in self.unready_gates:
            gate.addr = self.unready_gates.pop(gate.identity, None)
        self.workers[gate.identity] = gate
        for service in gate.work_services:
            if service not in self.services:
                self.services[service] = deque()
            self.services[service].append(gate.identity)
        self.idle_workers.appendleft(gate.identity)
        self.running = True

    def remove_worker(self, gate: Optional[RemoteGateRPC]) -> None:
        # for gates in self.services.values():
        #     if gate.identity in gates:
        #         gates.remove(gate.identity)
        if gate.identity in self.workers:
            self.workers.pop(gate.identity)
        if gate.identity in self.idle_workers:
            self.idle_workers.remove(gate.identity)
        for service in gate.work_services:
            if gate.identity in (idle_workers := self.services[service]):
                idle_workers.remove(gate.identity)
        if not self.workers:
            self.running = False

    def get_workers(self) -> dict[bytes, Union[RemoteGateRPC]]:
        return self.workers

    async def acquire_idle_worker(self) -> Optional[RemoteGateRPC]:
        return await self.acquire_service_idle_worker(self.name)

    async def acquire_service_idle_worker(
        self, service: str
    ) -> Optional[RemoteGateRPC]:
        if service == self.name:
            idle_gates = self.idle_workers
        else:
            idle_gates = self.services[service]
        try:
            while 1:
                gate = self.workers[(g_id := idle_gates.pop())]
                self.idle_workers.remove(g_id)
                if gate.is_alive() and gate.max_allowed_request != 0:
                    gate.max_allowed_request -= 1
                    return gate
        except IndexError:
            return None

    async def release_worker(self, gate: RemoteGateRPC):
        return await self.release_service_worker(self.name, gate)

    async def release_service_worker(
        self, service: str,
        gate: RemoteGateRPC
    ):
        if (
                gate.identity in self.workers
                and gate.is_alive()
        ):
            if service == self.name:
                idle_gates = self.idle_workers
            else:
                idle_gates = self.services[service]
            if gate.identity not in idle_gates:
                gate.max_allowed_request += 1
                idle_gates.appendleft(gate.identity)


class AMajordomo:
    """
    异步管家模式
    Asynchronous Majordomo
    https://zguide.zeromq.org/docs/chapter4/#Asynchronous-Majordomo-Pattern
    多偏好服务集群，连接多个提供不同服务的 Service（远程的 Service 也可能是一个代理，进行负载均衡）
    """

    # ClusterServiceClass = GateClusterService
    RemoteServiceClass = Service
    RemoteWorkerClass = RemoteWorker

    def __init__(
        self,
        *,
        identity: str = "gm",
        context: Context = None,
        heartbeat: int = None,
        gate_zap_mechanism: Union[str, bytes] = None,
        gate_zap_credentials: tuple = None,
        thread_executor: ThreadPoolExecutor = None,
        process_executor: ProcessPoolExecutor = None
    ):
        """
        :param identity: 用于在集群时的识别id
        :param context: 用于在使用 inproc 时使用同一上下文
        :param heartbeat: 需要和后端服务保持的心跳间隔，单位是毫秒。
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        self.identity = f"{identity}-{uuid4().hex}".encode("utf-8")
        if not context:
            context = Context()
        self.ctx = context
        set_ctx(self.ctx)
        # frontend
        self.frontend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        self.frontend.set(z_const.IDENTITY, self.identity)
        set_sock(self.frontend)
        self.clients = set()
        self.frontend_tasks = set()
        # backend
        self.backend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        set_sock(self.backend)
        self.backend.set(z_const.IDENTITY, self.identity)
        self.services: dict[str, ABCService] = dict()
        self.workers: dict[bytes, Union[RemoteWorker, RemoteGateRPC]] = dict()
        self.backend_tasks = set()
        # zap
        self.zap: Optional[z_aio.Socket] = None
        # TODO: ZAP domain
        self.zap_addr: str = ""
        self.zap_domain: bytes = b""
        self.zap_replies: dict[bytes, asyncio.Future] = dict()
        self.zap_cache = LRUCache(128)
        self.zap_tasks = set()
        # gate
        self.gate = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        set_sock(self.gate)
        self.gate.set(z_const.IDENTITY, self.identity)
        self.gate_cluster = GateClusterService(self)
        self.gate_replies: dict[bytes, asyncio.Future] = dict()
        self.gate_tasks = set()
        # 被转发的客户端请求和其他代理的请求和客户端id或其他地理的id的映射
        self.request_map: dict[bytes, bytes] = dict()
        gate_zap_mechanism, gate_zap_credentials = check_zap_args(
            gate_zap_mechanism, gate_zap_credentials
        )
        if gate_zap_mechanism:
            self.gate_zap_frames = [gate_zap_mechanism, *gate_zap_credentials]
        else:
            self.gate_zap_frames = []
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
        self.internal_tasks = set()
        # TODO: 用于跟踪客户端的请求，如果处理请求的后端服务掉线了，向可用的其他后端服务重发。
        self.cache_request = dict()
        self.max_process_tasks = Settings.MESSAGE_MAX
        self.running = False
        self._poller = z_aio.Poller()
        self._broker_task: Optional[asyncio.Task] = None
        self.register_internal_process()

    def bind_frontend(self, addr: str) -> None:
        """
        绑定前端地址
        """
        self.frontend.bind(check_socket_addr(addr))

    def unbind_frontend(self, addr: str) -> None:
        self.frontend.unbind(check_socket_addr(addr))

    def bind_backend(self, addr: str = None) -> None:
        if not addr:
            addr = Settings.WORKER_ADDR
        self.backend.bind(check_socket_addr(addr))

    def unbind_backend(self, addr: str) -> None:
        self.backend.unbind(check_socket_addr(addr))

    def bind_gate(self, addr: str) -> None:
        self.gate.bind(check_socket_addr(addr))

    def unbind_gate(self, addr: str) -> None:
        self.gate.unbind(check_socket_addr(addr))

    async def connect_gate(self, addr: str) -> None:
        gate_c = self.ctx.socket(z_const.REQ, z_aio.Socket)
        gate_c.set(z_const.SNDTIMEO, Settings.HEARTBEAT)
        gate_c.set(z_const.RCVTIMEO, Settings.HEARTBEAT)
        try:
            gate_c.connect(check_socket_addr(addr))
            await asyncio.sleep(1)
            connect_req = (
                Settings.GATE_MEMBER,
                Settings.GATE_COMMAND_RING,
                *self.gate_zap_frames,
            )
            connect_req = tuple(to_bytes(s) for s in connect_req)
            await gate_c.send_multipart(connect_req)
            replies = await gate_c.recv_multipart()
        except z_error.Again as error:
            error = (
                error.__repr__(),
                ''.join(format_tb(error.__traceback__))
            )
            Mlogger.error("\n".join(error))
            replies = None
        finally:
            gate_c.disconnect(addr)
            gate_c.close()
        if replies:
            try:
                (
                    GATE_version,
                    command,
                    *messages,
                ) = replies
                if messages := parse_zap_frame(messages):
                    mechanism, credentials, messages = messages
                gate_id = messages[0]
            except ValueError:
                return
            else:
                if gate_id in self.gate_cluster.get_workers():
                    return
                self.gate.connect(addr)
                await asyncio.sleep(0)
                await self.send_to_gate(
                    gate_id,
                    Settings.GATE_COMMAND_READY,
                    (msg_pack(list(self.services.keys())),)
                )
                self.gate_cluster.unready_gates[gate_id] = addr

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
            # Mlogger.info(f"Connecting to zap at {zap_addr}")
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
        gts = [
            self.now_ban(gate)
            for gate in self.gate_cluster.get_workers().values()
        ]
        await asyncio.gather(
            *wts, *gts
        )
        # for work in list(self.workers.values()):
        #     await self.now_ban(work)
        # for gate in self.gate_cluster.get_workers().values():
        #     await self.now_ban(gate)

    def close(self):
        self.thread_executor.shutdown()
        self.process_executor.shutdown()

    def run(self):
        if not self._broker_task:
            self._broker_task = self._loop.create_task(self._broker_loop())

    def stop(self):
        if self._broker_task:
            if not self._broker_task.cancelled():
                self._broker_task.cancel()
            self._broker_task = None
        self._poller.unregister(self.frontend)
        self._poller.unregister(self.backend)
        self._poller.unregister(self.gate)
        self.frontend.close()
        self.backend.close()
        self.gate.close()
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
        self, worker_id, max_allowed_request, socket, mdp_version, service
    ):
        worker = self.RemoteWorkerClass(
            worker_id, self.heartbeat,
            max_allowed_request, socket,
            mdp_version, service
        )
        worker.prolong(self._loop.time())
        worker.ready = True
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
                worker.identity,
                Settings.MDP_COMMAND_DISCONNECT
            )
            worker.ready = False
            worker.heartbeat_task.cancel()
        worker.service.remove_worker(worker)
        self.workers.pop(worker.identity, None)
        return True

    async def register_gate(
        self, gate_id, socket, mdp_version, work_services
    ):
        gate = RemoteGateRPC(
            gate_id, self.heartbeat,
            socket, mdp_version,
            self.gate_cluster,
            work_services
        )
        gate.prolong(self._loop.time())
        gate.ready = True
        gate.destroy_task = self._loop.create_task(
            self.ban_at_expiration(gate)
        )
        self.gate_cluster.add_worker(gate)

    async def ban_gate(self, gate: RemoteGateRPC):
        """
        和 ban_worker 分开，方便以后加逻辑
        """
        Mlogger.warning(f"ban gate({gate.identity}).")
        if gate.is_alive():
            await self.send_to_gate(
                gate.identity,
                Settings.GATE_COMMAND_DISCONNECT,
            )
            gate.ready = False
            gate.heartbeat_task.cancel()
        self.gate_cluster.remove_worker(gate)
        if gate.addr:
            self.gate.disconnect(gate.addr)
        return True

    async def ban_at_expiration(
        self, worker: Union[RemoteWorker, RemoteGateRPC]
    ):
        while 1:
            if worker.expiry <= self._loop.time():
                if isinstance(worker, RemoteGateRPC):
                    await self.ban_gate(worker)
                elif isinstance(worker, self.RemoteWorkerClass):
                    await self.ban_worker(worker)
                return
            await asyncio.sleep(
                1e-3 * worker.heartbeat * worker.heartbeat_liveness
            )

    async def now_ban(self, worker: Union[RemoteWorker, RemoteGateRPC]):
        if worker.destroy_task:
            worker.destroy_task.cancel()
        if isinstance(worker, RemoteGateRPC):
            await self.ban_gate(worker)
        elif isinstance(worker, self.RemoteWorkerClass):
            await self.ban_worker(worker)

    async def delay_ban(self, worker: Union[RemoteWorker, RemoteGateRPC]):
        worker.prolong(self._loop.time())

    async def send_heartbeat(self, worker: Union[RemoteWorker, RemoteGateRPC]):
        while 1:
            time_from_last_message = (
                    self._loop.time() - worker.last_message_sent_time
            )
            if time_from_last_message >= 1e-3 * worker.heartbeat:
                if isinstance(worker, self.RemoteWorkerClass):
                    await self.send_to_backend(
                        worker.identity,
                        Settings.MDP_COMMAND_HEARTBEAT
                    )
                elif isinstance(worker, RemoteGateRPC):
                    await self.send_to_gate(
                        worker.identity,
                        Settings.MDP_COMMAND_HEARTBEAT
                    )
                await asyncio.sleep(1e-3 * worker.heartbeat)
            else:
                await asyncio.sleep(
                    1e-3 * worker.heartbeat - time_from_last_message
                )

    def cleanup_internal_task(self, task: asyncio.Task):
        self.internal_tasks.remove(task)
        if exception := task.exception():
            exception_info = ''.join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Mlogger.warning(f"internal task exception:\n{exception_info}")

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

    def cleanup_gate_task(self, task: asyncio.Task):
        self.gate_tasks.remove(task)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception.__repr__()}"
                              f"\n{exception_info}")
            Mlogger.warning(f"gate task exception:\n{exception_info}")

    async def internal_process(
        self, client_id: bytes, request_id: bytes, func_name, args, kwargs
    ):
        reply = None
        exception = None
        kwargs.update({
            "client_id": client_id,
            "request_id": request_id,
        })
        try:
            if not self.internal_func.get(func_name, None):
                if Settings.SECURE:
                    return
                raise AttributeError(
                    "Function not found: {0}".format(func_name)
                )
            func = getattr(self, func_name)
            # Mlogger.debug(
            #     f"internal process, "
            #     f"func_name: {func_name}, "
            #     f"args: {args}, "
            #     f"kwargs: {kwargs}"
            # )
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
        if client_id in self.clients:
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
        elif client_id in self.gate_cluster.get_workers():
            if isinstance(reply, HugeData):
                task = self._loop.create_task(
                    self.reply_hugedata(
                        client_id, request_id, self.reply_gate, reply
                    ),
                    name=f"MajordomoReplyGateHD-{client_id}-{request_id}"
                )
            elif isinstance(reply, AsyncGenerator):
                task = self._loop.create_task(
                    self.reply_agenerator(
                        client_id, request_id, self.reply_gate, reply
                    ),
                    name=f"MajordomoReplyGateAG-{client_id}-{request_id}"
                )
            else:
                task = self._loop.create_task(
                    self.reply_gate(
                        client_id, Settings.MDP_INTERNAL_SERVICE,
                        request_id, msg_pack(reply)
                    )
                )
            self.gate_tasks.add(task)
            task.add_done_callback(self.cleanup_gate_task)

    async def reply_hugedata(self, client_id, request_id, reply_func, hugedata):
        try:
            async for data in hugedata.compress(self._loop):
                await reply_func(
                    client_id,
                    Settings.MDP_INTERNAL_SERVICE,
                    request_id,
                    Settings.STREAM_HUGE_DATA_TAG,
                    data
                )
        except Exception as e:
            await reply_func(
                client_id,
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
                client_id,
                Settings.MDP_INTERNAL_SERVICE,
                request_id,
                Settings.STREAM_HUGE_DATA_TAG,
                e
            )
        await reply_func(
            client_id,
            Settings.MDP_INTERNAL_SERVICE,
            request_id,
            Settings.STREAM_HUGE_DATA_TAG,
            hugedata.end_tag
        )
        hugedata.destroy()

    async def reply_agenerator(
        self, client_id, request_id, reply_func, agenerator: AsyncGenerator
    ):
        try:
            async for sub_reply in agenerator:
                messages = msg_pack(sub_reply)
                await reply_func(
                    client_id,
                    Settings.MDP_INTERNAL_SERVICE,
                    request_id,
                    Settings.STREAM_GENERATOR_TAG,
                    messages
                )
            await reply_func(
                client_id,
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
                client_id,
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
                empty,
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

    async def process_messages_from_gate(self, messages):
        try:
            (
                gate_id,
                empty,
                gate_version,
                command,
                *messages
            ) = messages
            if empty or gate_version != Settings.GATE_MEMBER:
                return
        except ValueError:
            return

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
            else:
                return
        else:
            mechanism = credentials = None

        # Mlogger.debug(
        #     f"gate id: {gate_id}\n"
        #     f"command: {command}\n"
        #     f"messages: {messages}\n"
        # )

        if not (gate := self.gate_cluster.get_workers().get(gate_id, None)):
            Mlogger.warning(f"The gate({gate_id}) was not registered.")
        if command == Settings.GATE_COMMAND_RING:
            if mechanism:
                request_id = b"gate_ring"
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
            self.gate_tasks.add(
                task := self._loop.create_task(
                    self.send_to_gate(
                        gate_id,
                        Settings.GATE_COMMAND_REPLY,
                        (self.identity,)
                    )
                )
            )
            task.add_done_callback(self.cleanup_gate_task)

        elif command == Settings.GATE_COMMAND_READY:
            if mechanism:
                request_id = b"gate_ready"
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
            if gate:
                return await self.now_ban(gate)
            try:
                work_services = msg_unpack(messages[0])
            except MsgUnpackError:
                return
            if gate_id not in self.gate_cluster.unready_gates:
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.send_to_gate(
                            gate_id,
                            Settings.GATE_COMMAND_READY,
                            (msg_pack(list(self.services.keys())),)
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)
            await self.register_gate(
                gate_id, self.gate,
                gate_version, work_services
            )

        elif command == Settings.GATE_COMMAND_HEARTBEAT:
            if messages:
                return
            if mechanism:
                request_id = b"gate_heartbeat"
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
            else:
                zap_result = True
            if gate:
                if not gate.ready or not zap_result:
                    return await self.now_ban(gate)
                await self.delay_ban(gate)

        elif command == Settings.GATE_COMMAND_REQUEST:
            if len(self.gate_replies) > self.max_process_tasks:
                return
            try:
                (
                    service_name,
                    request_id,
                    *body
                ) = messages
            except MsgUnpackError:
                return
            if mechanism:
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
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
                self.internal_tasks.add(
                    task := self._loop.create_task(
                        self.internal_process(
                            gate_id, request_id, func_name, args, kwargs
                        ),
                        name=f"InternalFunc({func_name})-{gate_id}-{request_id}"
                    )
                )
                task.add_done_callback(self.cleanup_internal_task)
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

        elif command == Settings.GATE_COMMAND_REPLY:
            try:
                service_name, request_id, *body = messages
            except ValueError:
                return
            if mechanism:
                # 流式回复的request_id是不变的
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    Mlogger.warning(
                        f"ZAP verification of Gate({gate_id})"
                        f" fails,'{mechanism}' mechanism is used, and the "
                        f"credential is '{credentials}', and the request_id "
                        f"is '{request_id}'"
                    )
                    return
            if not gate:
                return
            if not gate.ready:
                return await self.now_ban(gate)
            await self.delay_ban(gate)
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
                elif client_id in self.gate_cluster.get_workers():
                    self.gate_tasks.add(
                        task := self._loop.create_task(
                            self.reply_gate(
                                client_id, service_name,
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
                        # huge_reply.add_data(body)

        elif command == Settings.GATE_COMMAND_DISCONNECT:
            if mechanism:
                request_id = b"gate_disconnect"
                zap_result = await self.request_zap(
                    gate_id, gate_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
            if gate:
                gate.ready = False
                gate.expiry = self._loop.time()
                await self.now_ban(gate)

    async def request_gate(
        self,
        request_id: bytes,
        service: str,
        body: tuple[Union[bytes, str], ...]
    ):
        if not self.gate_cluster.get_workers():
            raise GateUnAvailableError()
        elif gate := await self.gate_cluster.acquire_service_idle_worker(
                service
        ):
            option = (service, request_id)
            await self.send_to_gate(
                gate.identity,
                Settings.GATE_COMMAND_REQUEST,
                option,
                body
            )
        else:
            raise BusyGateError()

    async def reply_gate(
        self,
        gate_id: bytes,
        service: str,
        request_id: bytes,
        *body,
    ):
        await self.send_to_gate(
            gate_id,
            Settings.GATE_COMMAND_REPLY,
            (service, request_id),
            body
        )

    async def send_to_gate(
        self,
        gate_id,
        command: Union[bytes, str],
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[
            Union[bytes, str], tuple[Union[bytes, str], ...]] = None,
    ):
        _message = [
            gate_id,
            b"",
            Settings.GATE_MEMBER,
            command,
        ]
        if self.gate_zap_frames:
            _message.extend(self.gate_zap_frames)
        if option:
            _message.extend(option)
        if isinstance(messages, (tuple, list)):
            _message.extend(messages)
        elif isinstance(messages, (bytes, str)):
            _message.append(messages)
        messages = tuple(to_bytes(s) for s in messages)
        await self.gate.send_multipart(messages)

    async def process_replies_from_backend(self, replies: list):
        try:
            (
                worker_id,
                empty,
                mdp_version,
                command,
                *messages
            ) = replies
            if empty or mdp_version != Settings.MDP_WORKER:
                return
        except ValueError:
            return

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
            else:
                return
        else:
            mechanism = credentials = None
        if not (worker := self.workers.get(worker_id, None)):
            Mlogger.warning(f"The worker({worker_id}) was not registered.")
        # Mlogger.debug(
        #     f"worker id: {worker_id}\n"
        #     f"command: {command}\n"
        #     f"messages: {messages}\n"
        # )

        # MDP 定义的 Worker 命令
        if command == Settings.MDP_COMMAND_READY:
            if mechanism:
                request_id = b"worker_ready"
                zap_result = await self.request_zap(
                    worker_id, worker_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
            if len(messages) > 1:
                return
            if worker:
                return await self.now_ban(worker)
            try:
                service_name, service_description, max_allowed_request = (
                    from_bytes(messages[0])
                ).split(Settings.MDP_DESCRIPTION_SEP)
                max_allowed_request = int(max_allowed_request)
            except ValueError:
                return
            if service_name not in self.services:
                self.register_service(service_name, service_description)
            service = self.services[service_name]
            await self.register_worker(
                worker_id, max_allowed_request,
                self.backend, mdp_version, service
            )

        elif command == Settings.MDP_COMMAND_HEARTBEAT:
            if messages:
                return
            if mechanism:
                request_id = b"worker_heartbeat"
                zap_result = await self.request_zap(
                    worker_id, worker_id, request_id,
                    mechanism, credentials
                )
            else:
                zap_result = True

            if worker:
                if not worker.ready or not zap_result:
                    return await self.now_ban(worker)
                await self.delay_ban(worker)

        elif command == Settings.MDP_COMMAND_REPLY:
            try:
                client_id, empty, request_id, *body = messages
                if empty:
                    return
            except ValueError:
                return
            if mechanism:
                # 流式回复的request_id是不变的
                zap_result = await self.request_zap(
                    worker_id, worker_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    Mlogger.warning(
                        f"ZAP verification of Worker({worker_id})"
                        f" fails,'{mechanism}' mechanism is used, and the "
                        f"credential is '{credentials}', and the request_id "
                        f"is '{request_id}'"
                    )
                    return
            if not worker:
                return
            if not worker.ready:
                return await self.now_ban(worker)
            if client_id in self.clients:
                self.frontend_tasks.add(
                    task := self._loop.create_task(
                        self.reply_frontend(
                            client_id, worker.service.name, request_id, *body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_frontend_task)
            if client_id in self.gate_cluster.get_workers():
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.reply_gate(
                            client_id, worker.service.name, request_id, *body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)
            service = worker.service
            await service.release_worker(worker)
            await self.delay_ban(worker)

        elif command == Settings.MDP_COMMAND_DISCONNECT:
            if mechanism:
                request_id = b"worker_disconnect"
                zap_result = await self.request_zap(
                    worker_id, worker_id, request_id,
                    mechanism, credentials
                )
                if not zap_result:
                    return
            if len(messages) > 1:
                return
            if worker:
                worker.ready = False
                worker.expiry = self._loop.time()
                Mlogger.warning(
                    f"recv disconnect and ban worker({worker.identity})"
                )
                await self.now_ban(worker)

    async def request_backend(
        self,
        service_name: str,
        client_id: bytes,
        request_id: bytes,
        body: Union[list[Union[bytes, str], ...], tuple[Union[bytes, str], ...]]
    ):
        service = self.services.get(service_name)
        try:
            if not service or not service.running:
                raise ServiceUnAvailableError(service_name)
            elif worker := await service.acquire_idle_worker():
                option = (client_id, b"", request_id)
                self.backend_tasks.add(
                    task := self._loop.create_task(
                        self.send_to_backend(
                            worker.identity, Settings.MDP_COMMAND_REQUEST,
                            option, body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_backend_task)
            elif self.gate_cluster.get_workers():
                # 转发请求其他代理。
                self.request_map[request_id] = client_id
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.request_gate(
                            request_id, service_name, body
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)
            else:
                raise BusyWorkerError()
        except (
                ServiceUnAvailableError, GateUnAvailableError,
                BusyGateError, BusyWorkerError
        ) as exception:
            exception = await generate_reply(exception=exception)
            except_reply = msg_pack(exception)
            if client_id in self.clients:
                self.frontend_tasks.add(
                    task := self._loop.create_task(
                        self.reply_frontend(
                            client_id, service_name, request_id,
                            except_reply
                        )
                    )
                )
                task.add_done_callback(self.cleanup_frontend_task)
            elif client_id in self.gate_cluster.get_workers():
                self.gate_tasks.add(
                    task := self._loop.create_task(
                        self.reply_gate(
                            client_id, service_name, request_id,
                            except_reply
                        )
                    )
                )
                task.add_done_callback(self.cleanup_gate_task)

    async def send_to_backend(
        self,
        worker_id,
        command: bytes,
        option: tuple[Union[bytes, str], ...] = None,
        messages: Union[list[Union[bytes, str], ...], tuple[Union[bytes, str], ...]] = None
    ):
        request = [
            worker_id,
            b"",
            Settings.MDP_WORKER,
            command,
        ]
        if option:
            request.extend(option)
        if isinstance(messages, (tuple, list)):
            request.extend(messages)
        elif isinstance(messages, (bytes, str)):
            request.append(messages)
        request = tuple(to_bytes(frame) for frame in request)
        await self.backend.send_multipart(request)
        return

    async def process_request_from_frontend(self, request: list):
        try:
            (
                client_id,
                empty,
                mdp_version,
                service_name,
                *messages
            ) = request
            if empty or mdp_version != Settings.MDP_CLIENT:
                return
        except ValueError:
            return

        if self.zap:
            if messages := parse_zap_frame(messages):
                mechanism, credentials, messages = messages
            else:
                return
        else:
            mechanism = credentials = None
        # Mlogger.debug(
        #     f"client id: {client_id}\n"
        #     f"service_name: {service_name}\n"
        #     f"messages: {messages}\n"
        # )
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
        if Settings.DEBUG:
            self.cache_request[request_id] = (
                service_name, client_id, request_id, body, self._loop.time()
            )
        else:
            self.cache_request[request_id] = (
                service_name, client_id, request_id, body
            )
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
            self.internal_tasks.add(
                task := self._loop.create_task(
                    self.internal_process(
                        client_id, request_id,
                        func_name, args, kwargs
                    ),
                    name=f"InternalFun({func_name})-{client_id}-{request_id}"
                )
            )
            task.add_done_callback(self.cleanup_internal_task)
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
        # Mlogger.debug(
        #     f"client id: {client_id}\n"
        #     f"request id: {request_id}\n"
        #     f"service_name: {service_name}\n"
        #     f"body: {body}\n"
        # )
        if request_id in self.cache_request:
            self.cache_request.pop(request_id)
            # request = self.cache_request.pop(request_id)
            # if Settings.DEBUG:
            #     Mlogger.debug(
            #         f"request({request_id}) use time from backend:"
            #         f" {loop.time() - request[-1]}"
            #     )
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
            self._poller.register(self.frontend, z_const.POLLIN)
            self._poller.register(self.backend, z_const.POLLIN)
            self._poller.register(self.gate, z_const.POLLIN)
            if self.zap:
                self._poller.register(self.zap, z_const.POLLIN)
            self.running = True

            while 1:
                socks = await self._poller.poll(self.heartbeat)
                socks = dict(socks)
                if self.backend in socks:
                    replies = await self.backend.recv_multipart()
                    bt = self._loop.create_task(
                        self.process_replies_from_backend(replies),
                    )
                    self.backend_tasks.add(bt)
                    bt.add_done_callback(self.cleanup_backend_task)

                if self.frontend in socks:
                    if len(self.frontend_tasks) >= self.max_process_tasks:
                        Mlogger.warning(
                            f"The number of pending requests from frontend has "
                            f"exceeded the cache limit for the requesting task."
                            f"The limit is {self.max_process_tasks} "
                            f"requests."
                        )
                        await asyncio.sleep(1)
                        continue
                    replies = await self.frontend.recv_multipart()
                    # 同后端任务处理。
                    ft = self._loop.create_task(
                        self.process_request_from_frontend(replies),
                    )
                    self.frontend_tasks.add(ft)
                    ft.add_done_callback(self.cleanup_frontend_task)

                if self.gate in socks:
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
            return True
        else:
            return False

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


class Client:

    def __init__(
        self,
        broker_addr: str = None,
        identity: str = "gc",
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

        self.identity = f"{identity}-{uuid4().hex}".encode("utf-8")
        self.ready: Optional[asyncio.Future] = None
        self._broker_addr = broker_addr
        self._recv_task: Optional[asyncio.Task] = None
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
        set_ctx(self.ctx)
        self.socket: Optional[z_aio.Socket] = None
        self._poller = z_aio.Poller()

        if not heartbeat:
            heartbeat = Settings.MDP_HEARTBEAT_INTERVAL
        self.heartbeat = heartbeat
        self.heartbeat_liveness = Settings.MDP_HEARTBEAT_LIVENESS
        self.heartbeat_expiry = self.heartbeat * self.heartbeat_liveness
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.last_message_sent_time: float = 0.0

        if not reply_timeout:
            reply_timeout = Settings.REPLY_TIMEOUT
        self.reply_timeout = reply_timeout

        self._executor = ThreadPoolExecutor()
        self.replies: dict[bytes, asyncio.Future] = dict()
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
        set_sock(self.socket)
        self.socket.set(z_const.IDENTITY, self.identity)
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
            self._recv_task.cancel()
        if self._executor:
            self._executor.shutdown(False)
            self._executor = None
        self._recv_task = None
        self.ready = None

    async def _send_to_majordomo(
        self, service_name: Union[str, bytes], *body,
    ) -> bytes:
        """
                客户端使用 DEALER 类型 socket，每个请求都有一个id，
                worker 每个回复都会带上这个请求id。 客户端需要将回复和请求对应上。
                :param service_name: 要访问的服务的名字
                """
        request_id = to_bytes(uuid4().hex)
        request = (
            b"",
            Settings.MDP_CLIENT,
            service_name,
            *self.zap_frames,
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
                if reply._exit:
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
        st = self._loop.time()
        request_id = await self._send_to_majordomo(
            service_name,
            msg_pack(func_name), msg_pack(args), msg_pack(kwargs)
        )
        self.replies[request_id] = self._loop.create_future()
        try:
            response = await asyncio.wait_for(
                self.replies[request_id],
                self.reply_timeout
            )
            # if Settings.DEBUG:
                # Clogger.debug(
                #     f"request(request_id) use time from majordomo:"
                #     f" {self._loop.time() - st}"
                # )
                # Clogger.debug(f"response type: {type(response)}")
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
        await asyncio.wait_for(self.ready, self.reply_timeout)
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
            logger.warning(msg)
        return await self._request(service_name, func_name, args, kwargs)

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
                await self._send_to_majordomo(
                    Settings.MDP_INTERNAL_SERVICE,
                    msg_pack("keepalive"), msg_pack((b"heartbeat",))
                )
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

    async def _query_service(self, service_name: bytes):
        return await self._majordomo_service(
            "query_service", service_name
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
                        empty,
                        mdp_client_version,
                        service_name,
                        request_id,
                        *body
                    ) = messages
                    if empty:
                        continue
                except ValueError:
                    continue
                if mdp_client_version != Settings.MDP_CLIENT:
                    continue
                try:
                    majordomo_liveness = self.heartbeat_liveness
                    if request_id not in self.replies:
                        continue
                    elif len(body) == 1:
                        body = msg_unpack(body[0])
                        self.replies[request_id].set_result(body)
                    elif len(body) > 1:
                        *option, body = body
                        if option[0] == Settings.STREAM_GENERATOR_TAG:
                            if not (reply := self.replies[request_id]).done():
                                stream_reply = StreamReply(
                                    Settings.STREAM_END_TAG,
                                    maxsize=self.stream_reply_maxsize,
                                    timeout=self.reply_timeout
                                )
                                self.replies[request_id].set_result(
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
                                    blksize=Settings.HUGE_DATA_SIZEOF
                                )
                                self.replies[request_id].set_result(huge_reply)
                            else:
                                huge_reply = reply.result()
                            if (len(option) > 1 and option[1] ==
                                    Settings.STREAM_EXCEPT_TAG):
                                huge_reply._remote_exception = body
                            else:
                                await self._loop.run_in_executor(
                                    self._executor, huge_reply.add_data, body
                                )
                except MsgUnpackError:
                    continue
        except asyncio.CancelledError:
            Clogger.warning("recv task cancelled.")
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            Clogger.error(exception)
            raise
        finally:
            await self.disconnect()
            self._heartbeat_task.cancel()
            self.ready = None

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
