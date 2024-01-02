# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/3/30 11:09
import functools
import secrets
import inspect
import os
import sys
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Coroutine, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from time import time
from .global_settings import Settings
from logging import getLogger


import asyncio
from traceback import format_exception, format_tb
from typing import Annotated, Union, overload, Optional
from uuid import uuid4
from gettext import gettext as _

import zmq.constants as z_const
import zmq.asyncio as z_aio
from zmq.auth.asyncio import AsyncioAuthenticator

from .exceptions import BuysWorkersError, RemoteException, ServiceUnAvailableError
from .mixins import _LoopBoundMixin
from .utils import (
    BoundedDict, check_socket_addr, from_bytes, gen_reply, interface,
    msg_pack, msg_unpack, to_bytes,
)


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
    heartbeat_liveness = 3
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
        self, identity: bytes, address: bytes, heartbeat: int, service: "Service"
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

    def is_alive(self):
        if time() > self.expiry:
            return False
        return True

# TODO: 发送消息的部分，需要测试是否可以从list改成tuple


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
        executor: Union[ThreadPoolExecutor, ProcessPoolExecutor] = None
    ):
        """
        :param broker_addr: 要连接的代理地址
        :param service: Service 实例
        :param identity: id
        :param heartbeat: 心跳间隔，单位是毫秒
        :param context: 用于创建socket的zeroMQ的上下文
        :param executor: 用于执行协程的执行器
        """
        self.heartbeat = heartbeat
        self.expiry = self.heartbeat_liveness * self.heartbeat
        if not context:
            self._ctx = Context()
        else:
            self._ctx = context
        self._socket = None
        self._poller = z_aio.Poller()
        self._broker_addr = broker_addr
        self.executor = executor
        if not identity:
            identity = uuid4().hex
        self.identity = f"gw-{identity}".encode("utf-8")
        self.service = service
        self.ready = False
        self.interfaces: dict = self.get_interfaces()
        self.requests = BoundedDict(
            maxsize=Settings.MESSAGE_MAX, timeout=0.1
        )
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
                _("Connecting to broker at {0} ...").format(self._broker_addr)
            )
            self._socket = self._ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
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
            logger.warning(_("Disconnecting from broker"))
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
        self.executor.shutdown()

    async def stop(self):
        if self.is_alive():
            if not self._recv_task.cancelled():
                self._recv_task.cancel()
            await self.disconnect()
            self.close()
            self.ready = False

    def release_request(self, task_name):
        task: asyncio.Task = self.requests.pop(task_name)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.error(
                f"process request task exception:\n{exception_info}"
            )

    async def send_to_broker(
        self,
        command: bytes,
        option: list[Union[bytes, str]] = None,
        message: list[Union[bytes, str]] = None
    ):
        """
        在MDP的Worker端发出的消息的命令帧后面加一个worker id帧，用作掉线重连，幂等。
        """
        if not self._socket:
            return
        if not message:
            message = []
        if not option:
            option = []
        message = [
            b"",
            Settings.MDP_WORKER,
            command,
            self.identity,
            *option,
            *message
        ]
        message = [await to_bytes(s) for s in message]
        logger.debug(_("send a message to broker: {0}").format(message))
        await self._socket.send_multipart(message)

    async def process_request(
        self, cli_id: bytes, cli_addr: bytes, request_id: bytes,
        func_details: dict
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
                logger.error(
                    f'{_("Invalid function details")}: {func_details}'
                )
                raise AttributeError(_("Invalid function details"))
            if not self.interfaces.get(func_name, None):
                raise AttributeError(
                    _("Function not found: {0}").format(func_name)
                )
            func = getattr(self, func_name)
            if not asyncio.iscoroutinefunction(func):
                task = loop.run_in_executor(
                    self.executor,
                    functools.partial(func, *args, **kwargs)
                )
            else:
                coro = func(*args, **kwargs)
                task = loop.create_task(coro)
        except Exception as e:
            reply = await gen_reply(exception=e)
        else:
            reply = await gen_reply(task)
        option = [b"@".join((cli_id, cli_addr, request_id))]
        message = [await msg_pack(reply)]
        await self.send_to_broker(
            Settings.MDP_COMMAND_REPLY, option, message
        )

    async def recv(self):
        try:
            await self.connect()
            logger.info(_("Worker event loop starts running."))
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
                    message = await self._socket.recv_multipart()
                    broker_liveness = self.heartbeat_liveness
                    try:
                        (
                            empty,
                            mdp_worker_version,
                            command,
                            *message
                        ) = message
                        if empty:
                            continue
                    except ValueError:
                        logger.debug(
                            _(
                                "Worker({worker_id}) receives invalid message:"
                                "\n{message}"
                            ).format(worker_id=self.identity, message=message)
                        )
                        continue
                    if mdp_worker_version != Settings.MDP_WORKER:
                        logger.error(_("The second frame is not an MDP_WORKER"))
                        continue
                    if command == Settings.MDP_COMMAND_HEARTBEAT:
                        pass
                    elif command == Settings.MDP_COMMAND_DISCONNECT:
                        logger.info(f"{self.identity} disconnected")
                        break
                    elif command == Settings.MDP_COMMAND_REQUEST:
                        try:
                            client, empty, request_id, body = message
                            if empty:
                                raise ValueError
                            client_id, client_addr = client.split(b"@")
                        except ValueError:
                            logger.error(
                                _("Request frame invalid: {0}").format(message)
                            )
                            continue
                        details = await msg_unpack(body)
                        task = loop.create_task(
                            self.process_request(
                                client_id, client_addr,
                                request_id, details
                            )
                        )
                        await self.requests.aset(request_id, task)
                        task.add_done_callback(
                            lambda fut: self.release_request(request_id)
                        )
                    else:
                        logger.debug(_("Invalid MDP command."))
                else:
                    broker_liveness -= 1
                    if broker_liveness == 0:
                        logger.warning(_("Broker offline"))
                        if self._reconnect:
                            logger.warning(_("Reconnect to Majordomo"))
                            await self.disconnect()
                            self._recv_task = loop.create_task(self.recv())
                            break
        except asyncio.CancelledError:
            return
        except Exception:
            exception = "".join(format_exception(*sys.exc_info()))
            logger.error(exception)
            return

    def run(self):
        if not self._recv_task:
            loop = self._get_loop()
            self._recv_task = loop.create_task(self.recv())

    @interface
    async def atest(self, *args, **kwargs):
        loop = self._get_loop()
        return {
            "name": "async atest",
            "args": args,
            "kwargs": kwargs,
            "loop_time": loop.time()
        }

    @interface
    def test(self, *args, **kwargs):
        return {
            "name": "test",
            "args": args,
            "kwargs": kwargs,
            "loop_time": time()
        }


class IOWorker(Worker):
    def __init__(
        self,
        broker_addr: str,
        service: "Service",
        identity: str = None,
        heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        context: Context = None,
        executor_max_workers: int = None,
        thread_name_prefix: str = "",
        executor_initializer: Callable = None,
        executor_initargs: tuple = ()
    ):
        if not thread_name_prefix:
            thread_name_prefix = f"AsyncWorkThread_{identity}"
        executor = ThreadPoolExecutor(
            max_workers=executor_max_workers,
            thread_name_prefix=thread_name_prefix,
            initializer=executor_initializer,
            initargs=executor_initargs
        )
        super().__init__(
            broker_addr, service, identity,
            heartbeat, context, executor
        )


class CPUWorker(Worker):
    def __init__(
        self,
        broker_addr: str,
        service: "Service",
        identity: str = None,
        heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        context: Context = None,
        executor_max_workers: int = None,
        executor_mp_context=None,
        executor_initializer: Callable = None,
        executor_initargs: tuple = ()
    ):
        executor = ProcessPoolExecutor(
            max_workers=executor_max_workers,
            mp_context=executor_mp_context,
            initializer=executor_initializer,
            initargs=executor_initargs
        )
        super().__init__(
            broker_addr, service, identity,
            heartbeat, context, executor
        )


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
        logger.info(
            _("'{name}' has added a worker {worker_id}").format(
                name=self.name, worker_id=worker.identity
            )
        )
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
        preference: str,
        worker_class: Callable[..., Worker] = None,
        workers_addr: str = None,
        workers_heartbeat: int = Settings.MDP_HEARTBEAT_INTERVAL,
        executor_max_number: int = 1
    ) -> Worker:
        if preference not in ("io", "cpu"):
            raise ValueError(_("preference must be 'io' or 'cpu'"))
        if preference == "io":
            if not workers_addr:
                workers_addr = Settings.IO_WORKER_ADDR
            max_workers = max((os.cpu_count() or 1), 4, executor_max_number)
            if not worker_class:
                worker_class = IOWorker
            worker = worker_class(
                workers_addr,
                self,
                identity=f"{self.name}_IOWorker",
                heartbeat=workers_heartbeat,
                executor_max_workers=max_workers
            )
        else:
            if not workers_addr:
                workers_addr = Settings.CPU_WORKER_ADDR
            max_workers = max((os.cpu_count() or 1), executor_max_number)
            if not worker_class:
                worker_class = CPUWorker
            worker = worker_class(
                workers_addr,
                self,
                identity=f"{self.name}_CPUWorker",
                heartbeat=workers_heartbeat,
                executor_max_workers=max_workers
            )
        self.add_worker(worker)
        return worker


class AsyncZAPService(AsyncioAuthenticator):
    """
    TODO: 异步的 zap 身份验证服务
    """


class AMajordomo(_LoopBoundMixin):
    """
    异步管家模式
    Asynchronous Majordomo
    https://zguide.zeromq.org/docs/chapter4/#Asynchronous-Majordomo-Pattern
    多偏好服务集群，连接多个提供不同服务的 Service（远程的 Service 也可能是一个代理，进行负载均衡）
    TODO: 分布式计算，均衡任务到不同节点。
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
        self.frontend_tasks = BoundedDict(None, 10*Settings.ZMQ_HWM, 0.1)
        self.backend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        self.backend_tasks = BoundedDict(None, 10*Settings.ZMQ_HWM, 0.1)
        if not backend_addr:
            backend_addr = Settings.IO_WORKER_ADDR
        logger.debug(_("backend socket bound to: {0}").format(backend_addr))
        self.backend.bind(check_socket_addr(backend_addr))
        # majordomo
        self.heartbeat = heartbeat
        self.services: dict[str, ABCService] = dict()
        self.workers: dict[bytes, RemoteWorker] = dict()
        self.remote_service_class = Service
        self.remote_worker_class = RemoteWorker
        self.ban_timer_handle: dict[bytes, asyncio.Task] = dict()
        # TODO: 用于跟踪客户端的请求，如果处理请求的后端服务掉线了，向可用的其他后端服务重发。
        self.cache_request = dict()
        self.running = False
        self._poller = z_aio.Poller()
        self._broker_task: Optional[asyncio.Task] = None
        # zap
        self.zap_mechanism: str = ""
        self.zap_socket: Union[z_aio.Socket, None] = None
        self.zap_domain = zap_domain
        self.zap_replies: BoundedDict[str, list] = BoundedDict(
            None, 10*Settings.ZMQ_HWM, 1.5
        )
        if zap_mechanism:
            self.zap_mechanism = zap_mechanism
            if not zap_addr:
                zap_addr = Settings.ZAP_INPROC_ADDR
            if zap_addr := check_socket_addr(zap_addr):
                self.zap_socket = self.ctx.socket(z_const.DEALER, z_aio.Socket)
                self.zap_socket.connect(zap_addr)
                self.zap_request_id = 1
            else:
                raise ValueError(
                    "The ZAP mechanism is specified and "
                    "the ZAP server address must also be provided."
                )

    def bind(self, addr: str) -> None:
        """
        绑定到对外地址
        """
        self.frontend.bind(check_socket_addr(addr))

    def close(self):
        self.frontend.close()
        self.backend.close()

    def register_service(self, service: ABCService):
        """
        添加 service
        """
        self.services[service.name] = service

    def register_worker(self, worker: RemoteWorker):
        self.workers[worker.identity] = worker

    async def ban_worker(self, worker: RemoteWorker, wait: float = 0):
        """
        屏蔽掉指定 worker
        """
        if not wait:
            if worker.identity in self.ban_timer_handle:
                self.ban_timer_handle[worker.identity].cancel()
                self.ban_timer_handle.pop(worker.identity)
        else:
            await asyncio.sleep(wait)
        logger.warning(
            _("Ban the worker: {worker_id}({address}).").format(
                worker_id=worker.identity, address=worker.address
            )
        )
        if worker.is_alive():
            request = [
                await to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_DISCONNECT
            ]
            try:
                await self.backend.send_multipart(request)
            except Exception:
                pass
        worker.service.remove_worker(worker)
        if worker.identity not in self.workers:
            logger.warning(
                _(
                    "The worker '{worker_id}' has been banned."
                ).format(worker_id=worker.identity)
            )
            return True
        self.workers.pop(worker.identity, None)

        return True

    def ban_worker_after_expiration(self, worker: RemoteWorker):
        loop = self._get_loop()
        self.ban_timer_handle[worker.identity] = t = loop.create_task(
            self.ban_worker(worker, wait=worker.expiry)
        )
        t.add_done_callback(
            lambda fut: self.ban_timer_handle.pop(worker.identity)
        )

    async def internal_service(
        self,
        service_name: bytes, client_id: bytes, client_addr: bytes,
        request_id: bytes, body: bytes
    ):
        service = service_name.lstrip(Settings.MDP_INTERNAL_SERVICE_PREFIX)
        stat_code = b"501"
        if service == b"service":
            if await from_bytes(body) in self.services:
                stat_code = b"200"
            else:
                stat_code = b"404"
        elif service == b"client_heartbeat":
            if body == b"heartbeat":
                stat_code = b"200"
        reply = [
            client_addr,
            b"",
            Settings.MDP_CLIENT,
            service_name,
            client_id,
            request_id,
            stat_code,
        ]
        await self.reply_frontend(reply)

    async def request_zap(
        self, identity, address, request_id, mechanism, credentials
    ) -> bool:
        # TODO: 提供服务的远程 worker 也需要身份验证。
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
                     ] + credentials
        zap_frames = [await to_bytes(s) for s in zap_frames]
        failed_reply = {
                "user_id": identity,
                "metadata": "",
                "status_code": "500",
                "status_text": "ZAP Service Failed.",
            }
        try:
            await self.zap_socket.send_multipart(zap_frames)
        except asyncio.TimeoutError:
            zap_reply = failed_reply
        else:
            try:
                zap_reply = await self.zap_replies.aget(
                    f"{identity}-{request_id}"
                )
                self.zap_replies.pop(f"{identity}-{request_id}")
            except KeyError:
                zap_reply = failed_reply
        if zap_reply["status_code"] != b"200":
            logger.error(f"Authentication failure: {zap_reply['status_code']}")
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
            logger.error(_("ZAP replies frame invalid."))
            return
        await self.zap_replies.aset(
            f"{user_id}-{request_id}",
            {
                "user_id": user_id,
                "request_id": request_id,
                "metadata": metadata,
                "status_code": status_code,
                "status_text": status_text,
            }
        )

    def release_frontend_task(self, task_name):
        task: asyncio.Task = self.frontend_tasks.pop(task_name)
        if exception := task.exception():
            exception_info = ''.join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.error(
                f"frontend task exception:\n{exception_info}"
            )

    def release_backend_task(self, task_name):
        task: asyncio.Task = self.backend_tasks.pop(task_name)
        if exception := task.exception():
            exception_info = "".join(format_tb(exception.__traceback__))
            exception_info = (f"{exception.__class__}:{exception}"
                              f"\n{exception_info}")
            logger.error(
                f"backend task exception:\n{exception_info}"
            )

    async def process_replies_from_backend(self, replies: list):
        logger.debug(_("replies from backend: {0}").format("replies"))
        try:
            (
                worker_addr,
                empty,
                mdp_worker_version,
                command,
                worker_id,
                *message
            ) = replies
            if empty:
                raise ValueError
        except ValueError:
            logger.warning(f"invalid replies: {replies}")
            return
        if mdp_worker_version != Settings.MDP_WORKER:
            logger.debug(_("The second frame is not an MDP_WORKER"))
            return
        # MDP 定义的 Worker 命令
        if not (worker := self.workers.get(worker_id, None)):
            logger.warning(
                _("The worker({worker_id}) was not registered").format(
                    worker_id=worker_id
                )
            )

        if command == Settings.MDP_COMMAND_READY:
            if len(message) > 1:
                logger.error(
                    _(
                        "This message does not conform to the format of the "
                        "READY command."
                    )
                )
                return
            try:
                service_name, service_description = (
                    await from_bytes(message[0])
                ).split(":")
            except ValueError:
                logger.debug(_("Not found description for service frame"))
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
                logger.error(
                    _(
                        'The worker({0}) had already sent the "READY" command'
                    ).format(worker.identity)
                )
                return await self.ban_worker(worker)
            worker.ready = True
            service.add_worker(worker)
            self.register_worker(worker)
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_HEARTBEAT:
            if len(message) > 1:
                logger.error(
                    _(
                        "This message does not conform to the format of the "
                        "HEARTBEAT command."
                    )
                )
                return
            if not worker:
                return
            elif not worker.ready:
                logger.error(
                    _(
                        'The worker({0}) don\'t sent the "READY" command'
                    ).format(worker.identity)
                )
                return await self.ban_worker(worker)
            worker.expiry = 1e-3 * worker.heartbeat_liveness * worker.heartbeat + time()
            if t := self.ban_timer_handle.get(worker.identity):
                t.cancel()
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_REPLY:
            try:
                client, *body = message
                client_id, client_addr, request_id = client.split(b"@")
            except ValueError:
                logger.error(
                    _(
                        "This message does not conform to the format of the "
                        "REPLY command."
                    )
                )
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
            worker.ready = False
            worker.expiry = time()
            await self.ban_worker(worker)
        else:
            logger.error(_("Invalid command: {0}").format(command))

    async def reply_frontend(self, reply: list):
        reply = [await to_bytes(frame) for frame in reply]
        try:
            await self.frontend.send_multipart(reply)
            logger.debug(_("send reply to frontend: {0}").format(reply))
        except Exception as e:
            logger.error(
                _(
                    "The reply({reply}) sent to frontend failed.error:\n"
                    "{error}"
                ).format(reply=reply, error=e)
            )

    async def process_request_from_frontend(self, request: list):
        logger.debug(_("replies from frontend: {0}").format(request))
        try:
            (
                client_addr,
                empty,
                mdp_client_version,
                service_name,
                client_id,
                *message
            ) = request
            if empty:
                raise ValueError
        except ValueError:
            logger.warning(_("invalid request: {0}").format(request))
            return False

        if self.zap_socket:
            try:
                mechanism, *credentials, request_id, body = message
            except ValueError:
                logger.debug(_("ZAP frames invalid"))
                return False
            zap_result = await self.request_zap(
                client_id, client_addr, request_id, mechanism, credentials
            )
            if not zap_result:
                return False
        else:
            try:
                request_id, body = message
            except ValueError:
                logger.error(_("body frames invalid: {0}").format(message))
                return False
        logger.debug(_("request service: {0}").format(service_name))
        if service_name.startswith(Settings.MDP_INTERNAL_SERVICE_PREFIX):
            await self.internal_service(
                service_name, client_id, client_addr,
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
        service_name = await from_bytes(service_name)
        for wid, worker in self.workers.items():
            if not worker.is_alive():
                await self.ban_worker(worker)
        service = self.services.get(service_name)
        exception = await gen_reply(
            exception=ServiceUnAvailableError(service_name)
        )
        except_reply = [
                client_addr,
                b"",
                Settings.MDP_CLIENT,
                service_name,
                client_id,
                request_id,
                await msg_pack(exception)
        ]
        if not service or not service.running:
            logger.error(_("Service({0}) unavailable").format(service.name))
            await self.reply_frontend(except_reply)
        elif worker := service.acquire_idle_worker():
            request = [
                worker.address,
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_REQUEST,
                b"@".join((client_id, client_addr)),
                b"",
                request_id,
                body
            ]
            request = [await to_bytes(frame) for frame in request]
            try:
                logger.debug(_("send backend: {0}").format(request))
                await self.backend.send_multipart(request)
            except Exception as e:
                logger.error(
                    _(
                        "The request({request}) sent to backend failed.\n"
                        "error: {error}"
                    ).format(request=request, error=e)
                )
                await self.reply_frontend(except_reply)
        else:
            logger.error(f'"{service_name}" service has no idle workers.')
            exception = await gen_reply(exception=BuysWorkersError())
            except_reply[-1] = await msg_pack(exception)
            await self.reply_frontend(except_reply)

    async def send_heartbeat(self):
        loop = self._get_loop()
        for worker in self.workers.values():
            message = [
                await to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_HEARTBEAT
            ]
            fut = self.backend.send_multipart(message)
            if fut.done():
                continue
            ht = loop.create_task(fut)
            hn = f"heartbeat_task_{worker.identity}"
            await self.backend_tasks.aset(hn, ht)
            ht.add_done_callback(
                lambda fut: self.release_backend_task(hn)
            )

    async def _broker_loop(self):
        loop = self._get_loop()
        self._poller.register(self.frontend, z_const.POLLIN)
        self._poller.register(self.backend, z_const.POLLIN)
        if self.zap_socket:
            self._poller.register(self.zap_socket, z_const.POLLIN)
        self.running = True
        logger.debug(_("broker loop running."))

        while 1:
            try:
                socks = dict(await self._poller.poll(self.heartbeat))
                if self.backend in socks:
                    replies = await self.backend.recv_multipart()
                    # 后端异步任务，避免等待处理后端回复导致阻塞。
                    bt = loop.create_task(
                        self.process_replies_from_backend(replies)
                    )
                    bt_id = secrets.token_hex()
                    tn = f"backend_task_{bt_id}"
                    # bt_id += 1
                    await self.backend_tasks.aset(tn, bt)
                    bt.add_done_callback(
                        lambda fut: self.release_backend_task(tn)
                    )

                if self.frontend in socks:
                    replies = await self.frontend.recv_multipart()
                    # 同后端任务处理。
                    ft = loop.create_task(
                        self.process_request_from_frontend(replies)
                    )
                    ft_id = secrets.token_hex()
                    tn = f"frontend_task_{ft_id}"
                    # ft_id += 1
                    await self.frontend_tasks.aset(tn, ft)
                    ft.add_done_callback(
                        lambda fut: self.release_frontend_task(tn)
                    )

                if self.zap_socket and self.zap_socket in socks:
                    replies = await self.zap_socket.recv_multipart()
                    await self.process_replies_from_zap(replies)

                await self.send_heartbeat()
            except Exception:
                exception = "".join(format_exception(*sys.exc_info()))
                logger.error(exception)
                break

        await self.disconnect_all()
        self._poller.unregister(self.frontend)
        self._poller.unregister(self.backend)
        if self.zap_socket:
            self._poller.unregister(self.zap_socket)
        logger.info(_("broker loop exit."))

    async def disconnect_all(self):
        # TODO: 需要通知客户端吗？
        logger.warning(_("Disconnected and all the workers."))
        for work in self.workers.values():
            await self.ban_worker(work)

    def run(self):
        if not self._broker_task:
            loop = self._get_loop()
            self._broker_task = loop.create_task(self._broker_loop())

    def stop(self):
        if self._broker_task:
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
        reply_timeout: float = Settings.MDP_REPLY_TIMEOUT,
        msg_max: int = Settings.MESSAGE_MAX,
        zap_mechanism: str = None,
        credential_frames: list = None,
    ):
        """
        客户端，
        TODO：连接远程的 MDP 服务，连接后发送一条查询有哪些服务的消息，
          然后将包含的服务添加为客户端关联的服务实例，链式调用服务包含的方法。
        TODO: 对于没有收到回复的请求记录并保存下来，可以设置重试次数来重新请求。
        TODO: 从rpc获取的所有函数，在客户端这里都用这个生成对应的本地函数。
        :param context: zmq 的上下文实例，为空则创建。
        :param heartbeat: 维持连接的心跳消息间隔，默认为 MDP_HEARTBEAT，单位是毫秒。
        :param reply_timeout: 等待回复的超时时间
        :param msg_max: 保存发送或接收到的消息的最大数量，默认为 MSSAGE_MAX。
        :param zap_mechanism: zap 的验证机制。
        :param credential_frames: zap 的凭据帧列表，不填则为空列表。
        """
        self._broker_addr = broker_addr
        if not identity:
            identity = uuid4().hex
        self.identity = f"gc-{identity}".encode("utf-8")
        self.ready = False

        if not context:
            self.ctx = Context()
        else:
            self.ctx = context
        self.socket = None
        self._poller = z_aio.Poller()

        self.heartbeat = heartbeat
        self.heartbeat_expiry = heartbeat * Settings.MDP_HEARTBEAT_LIVENESS
        self.reply_timeout = reply_timeout

        self.requests: BoundedDict[str, list] = BoundedDict(
            None, msg_max, 1.5
        )
        self._recv_task = None

        self.default_service = Settings.SERVICE_DEFAULT_NAME
        self._temp_service = self._temp_func = None

        if credential_frames is None:
            credential_frames = list()
        if (zap_mechanism == Settings.ZAP_MECHANISM_NULL
                and len(credential_frames) != 0):
            AttributeError(
                f'The "{Settings.ZAP_MECHANISM_NULL}" mechanism '
                f'should not have credential frames.'
            )
        elif (zap_mechanism == Settings.ZAP_MECHANISM_PLAIN
              and len(credential_frames) != 2):
            AttributeError(
                f'The "{Settings.ZAP_MECHANISM_PLAIN}" mechanism '
                f'should have tow credential frames: '
                f'a username and a password.'
            )
        elif zap_mechanism == Settings.ZAP_MECHANISM_CURVE:
            raise RuntimeError(
                f'The "{Settings.ZAP_MECHANISM_CURVE}"'
                f' mechanism is not implemented yet.'
            )
        elif zap_mechanism:
            raise ValueError(
                f'mechanism can only be '
                f'"{Settings.ZAP_MECHANISM_NULL}" or '
                f'"{Settings.ZAP_MECHANISM_PLAIN}" or '
                f'"{Settings.ZAP_MECHANISM_CURVE}"'
            )
        if not zap_mechanism:
            self.zap_frames = []
        else:
            self.zap_frames = [self.identity, zap_mechanism] + credential_frames
        self.connect()

    def connect(self):
        if not self.ready:
            loop = self._get_loop()
            logger.info(
                _("Connecting to broker at {0} ...").format(self._broker_addr)
            )
            self.socket = self.ctx.socket(
                z_const.DEALER, z_aio.Socket
            )
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
            self._recv_task.cancel()
            self._recv_task = None
            self.disconnect()
            self.ready = False

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
        request = [await to_bytes(frame) for frame in request]
        logger.debug(f"request: {request}")
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
        body = await msg_pack(
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
        except KeyError:
            raise TimeoutError(
                _("Timeout while requesting the Majordomo service.")
            )
        if exception := response.get("exception"):
            raise RemoteException(*exception)
        return response.get("result")

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
        while 1:
            try:
                if not self.ready:
                    break
                if loop.time() > heartbeat_at:
                    await self._send_heartbeat()
                    heartbeat_at = 1e-3 * self.heartbeat + loop.time()
                socks = await self._poller.poll(self.heartbeat)
                if socks:
                    message = await self.socket.recv_multipart()
                    try:
                        (
                            empty,
                            mdp_client_version,
                            service_name,
                            client_id,
                            request_id,
                            body
                        ) = message
                        if empty:
                            continue
                    except ValueError:
                        logger.debug(_("invalid message: {0}").format(message))
                        continue
                    logger.debug(
                        f"Reply: \n"
                        f"frame 0: {empty},\n"
                        f"frame 1: {mdp_client_version},\n"
                        f"frame 2: {service_name},\n"
                        f"frame 3: {client_id},\n"
                        f"frame 4: {request_id},\n"
                        f"frame 5: {body}"
                    )
                    if mdp_client_version != Settings.MDP_CLIENT:
                        logger.error(_("The second frame is not an MDP_CLIENT"))
                        continue
                    if client_id != self.identity:
                        continue
                    if service_name == b"gate.client_heartbeat":
                        continue
                    body = await msg_unpack(body)
                    await self.requests.aset(request_id, body)
            except Exception:
                exception = "".join(format_exception(*sys.exc_info()))
                logger.error(exception)

    def __getattr__(self, item):
        if not self._temp_func:
            self._temp_service = self.default_service
        else:
            if self._temp_service == self.default_service:
                self._temp_service = self._temp_func
            else:
                self._temp_service = ".".join(
                    (self._temp_service, self._temp_func)
                )
        self._temp_func = item
        return self

    def __call__(self, *args, **kwargs) -> Coroutine:
        """
        返回一个执行rpc调用的协程
        """
        waiting = self.knock(
                self._temp_service, self._temp_func, *args, **kwargs
            )
        self._temp_service = self._temp_func = None
        return waiting
