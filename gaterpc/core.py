# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/3/30 11:09
import asyncio
import inspect
import os
import sys
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop, Queue, get_running_loop
from collections import deque
from logging import getLogger
from multiprocessing import Process
from pprint import pformat
from threading import Thread
from time import time
from traceback import format_exception
from typing import Callable, Dict, Union, overload
from uuid import uuid4

import zmq.constants as z_const
import zmq.asyncio as z_aio
from zmq import Frame
from zmq.auth.asyncio import AsyncioAuthenticator

from .exceptions import BuysWorkersError, ServiceUnAvailableError
from gaterpc.conf.global_settings import Settings
from .utils import (
    BoundedDict, check_socket_addr, from_bytes, gen_reply, msg_dump,
    msg_load, to_bytes,
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
    Worker 的抽象基类，可以根据任务需求设置是否限制接收的请求数量。
    """
    max_allowed_request_queue: int = 0
    request_queue: Queue

    @abstractmethod
    def is_alive(self):
        pass


class RemoteWorker(ABCWorker):
    def __init__(
        self, identity: str, address: str, heartbeat: float, service: str
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
        self.expiry = time() + heartbeat
        self.address: str = address

    def is_alive(self):
        if time() > self.expiry:
            return False
        return True


class Worker(ABCWorker):
    """
    所有要给远端调用的方法都需要用interface函数装饰。
    """

    def __init__(
        self, identity: str,
        heartbeat: float,
        broker_addr: str,
        service: str,
        context: Context = None
    ):
        self.identity = f"gw-{identity}"
        self.service = service
        self._heartbeat = heartbeat
        self.ready = False
        if not context:
            self._ctx = Context()
        else:
            self._ctx = context
        self._socket = self._ctx.socket(z_const.DEALER, z_aio.Socket)
        self._broker_addr = broker_addr
        self.interfaces: dict = self.get_interface()

    def get_interface(self):
        return {
            name: {
                "doc": inspect.getdoc(method),
                "signature": inspect.signature(method)
            }
            for name, method in inspect.getmembers(self)
            if inspect.ismethod(method)
            and not getattr(method, "__interface__")
        }

    def is_alive(self):
        return self.ready

    def connect(self):
        self._socket.connect(self._broker_addr)

    def disconnect(self):
        self._socket.disconnect(self._broker_addr)

    async def send_heartbeat(self):
        message = [
            b"",
            Settings.MDP_WORKER,
            Settings.MDP_COMMAND_HEARTBEAT,
        ]
        return await self._socket.send_multipart(message)

    def close(self):
        """
        关闭打开的资源。
        """
        self._socket.close()

    def stop(self):
        self.close()
        self.disconnect()

    async def run(self):
        # TODO: 从socket接收任务，socket状态异常时退出，当接收到停止信号时，停止并退出。
        # TODO: 定时发送心跳信息到Server的backend，任何回复都要带心跳。
        self.ready = True
        try:
            poller = z_aio.Poller()
            poller.register(self._socket, z_const.POLLIN)
            while 1:
                socks = dict(await poller.poll(self._heartbeat))
                if socks.get(self._socket, None) == z_const.POLLIN:
                    request = await self._socket.recv_multipart()
        finally:
            self.stop()


class IOWorker(Thread):
    def __init__(
        self,
        worker_class: Callable[[str, float, str, str, Context], Worker],
        worker_identity: str,
        worker_heartbeat: float,
        broker_addr: str,
        service_name: str,
        loop: AbstractEventLoop = None,
        context: Context = None
    ):
        if not context:
            self.context = Context()
        else:
            self.context = context
        self._loop = loop
        self.worker_class = worker_class
        self.worker_identity = worker_identity
        self.worker_heartbeat = worker_heartbeat
        self.broker_addr = broker_addr
        self.worker_ready = False
        self.service_name = service_name
        name = f"AsyncWorkThread_{self.worker_identity}"
        super().__init__(group=None, name=name, daemon=False)

    def run(self) -> None:
        try:
            worker = self.worker_class(
                self.worker_identity, self.worker_heartbeat,
                self.broker_addr, self.service_name, self.context
            )
            worker.connect()
            if self._loop is not None:
                asyncio.set_event_loop(self._loop)
            asyncio.run(worker.run())
        finally:
            del self._target, self._args, self._kwargs

    def stop(self):
        return not self.is_alive()


class CPUWorker(Process):
    def __init__(
        self,
        worker_class: Callable[[str, float, str, str], Worker],
        worker_identity: str,
        worker_heartbeat: float,
        broker_addr: str,
        service_name: str,
    ):
        self.worker_class = worker_class
        self.worker_identity = worker_identity
        self.worker_heartbeat = worker_heartbeat
        self.broker_addr = broker_addr
        self.service_name = service_name
        self.worker_ready = False
        name = f"AsyncWorkProcess_{self.worker_identity}"
        super().__init__(group=None, name=name, daemon=False)

    def run(self) -> None:
        worker = self.worker_class(
            self.worker_identity, self.worker_heartbeat, self.broker_addr,
            self.service_name
        )
        worker.connect()
        asyncio.run(worker.run())

    def stop(self):
        self.kill()
        self.close()
        return True


class ABCService(ABC):
    """
    Service 的抽象基类
    """

    @abstractmethod
    def remove_worker(self, worker: ABCWorker):
        pass

    @abstractmethod
    def add_worker(self, worker: ABCWorker):
        pass


class RemoteService(ABCService):
    """
    在代理内部映射的远程服务，管理远程提供该服务的 worker。
    TODO: 如何避免有伪造的假服务连上来，会导致客户端调用本该有却没有的接口而出错。
    """
    def __init__(self, name: str, description: str):
        """
        :param name: 该服务的名称
        :param description: 描述该服务能提供的功能
        """
        self.name = name
        self.description = description
        self.workers: Dict[str, RemoteWorker] = dict()
        self.running = False
        self.idle_workers: deque[str] = deque()

    def remove_worker(self, worker: RemoteWorker):
        if worker.identity in self.workers:
            self.workers.pop(worker.identity)
        if worker.identity in self.idle_workers:
            self.idle_workers.remove(worker.identity)
        if not self.workers:
            self.running = False

    def add_worker(self, worker: RemoteWorker):
        if worker.identity in self.workers:
            return
        logger.debug(
            f"'{self.name}' adds one worker {worker.identity}"
        )
        worker.service = self.name
        self.workers[worker.identity] = worker
        self.idle_workers.append(worker.identity)
        if not self.running:
            self.running = True

    def get_idle_worker(self) -> Union[RemoteWorker, None]:
        for worker_id in list(self.idle_workers):
            if not self.workers[worker_id].is_alive():
                self.idle_workers.remove(worker_id)
            return self.workers[worker_id]
        return None


class Service(ABCService):
    """
    本地运行的服务，管理本地 worker。
    """
    def __init__(
        self, name: str,
        description: str,
        worker_class: Callable[..., Worker],
        preference: str = "io",
    ):
        """
        :param name: 该服务的名称
        :param description: 描述该服务能提供的功能
        :param worker_class: 包含可调用接口的工作类。
        :param preference: 该服务资源偏好是 cpu 密集任务还是 io 密集任务。
          io 密集型使用多线程，每一个线程使用一个新的事件循环。
          cpu密集型使用多进程。
        """
        self.name = name
        self.description = description
        if preference not in ("io", "cpu"):
            raise ValueError("preference must be 'io' or 'cpu'")
        self.preference = preference
        self.worker_class = worker_class
        self.workers: Dict[str, Union[IOWorker, CPUWorker]] = dict()
        self.idle_workers: deque[str] = deque()
        self.running = False

    def remove_worker(self, worker: Worker):
        if worker.identity in self.workers:
            self.workers[worker.identity].stop()
            self.workers.pop(worker.identity)

    def add_worker(self, worker: Worker):
        if worker.identity in self.workers:
            return
        logger.debug(
            f"'{self.name}' adds one worker {worker.identity}"
        )
        worker.service = self.name
        self.workers[worker.identity] = worker
        self.idle_workers.append(worker.identity)
        if not self.running:
            self.running = True

    def start_workers(
        self,
        workers_url: str = None,
        workers_heartbeat: float = Settings.MDP_HEARTBEAT,
        number: int = 1
    ):

        if not workers_url:
            workers_url = Settings.WORKER_CONF[self.preference]["workers_url"]
        if self.preference == "io":
            max_workers = max((os.cpu_count() or 1), 4, number)
            for i in range(max_workers):
                t = IOWorker(
                    worker_class=self.worker_class,
                    worker_identity=str(i),
                    worker_heartbeat=workers_heartbeat,
                    broker_addr=workers_url,
                    service_name=self.name
                )
                t.start()
                self.workers[t.worker_identity] = t
        else:
            max_workers = max((os.cpu_count() or 1), number)
            for i in range(max_workers):
                p = CPUWorker(
                    worker_class=self.worker_class,
                    worker_identity=str(i),
                    worker_heartbeat=workers_heartbeat,
                    broker_addr=workers_url,
                    service_name=self.name
                )
                p.start()
                self.workers[p.worker_identity] = p
        self.running = True

    def stop_workers(self):
        if self.running:
            # TODO: 发送停止消息。
            for w_id, worker in self.workers.items():
                worker.stop()

    async def run(
        self,
        broker_url: str,
        nbr_workers: int = 3
    ):
        """
        :param broker_url: 要连接的代理的 url
        :param nbr_workers: 工作类实例的并行数量。
        :return:
        """
        self.start_workers(broker_url, nbr_workers)


class AsyncZAPService(AsyncioAuthenticator):
    """
    TODO: 异步的 zap 身份验证服务
    """


class Server(object):
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
        backend_url: str = None,
        heartbeat: float = Settings.MDP_HEARTBEAT,
        service_class: Callable[..., RemoteService] = RemoteService,
        worker_class: Callable[..., RemoteWorker] = RemoteWorker,
        zap_mechanism: str = None,
        zap_addr: str = None,
        zap_domain: str = Settings.ZAP_DEFAULT_DOMAIN
    ):
        """
        :param backend_url: 要绑定后端服务的地址，为空则使用默认的本地服务地址。
        :param heartbeat: 需要和后端服务保持的心跳间隔。
        :param service_class: 用于表示提供服务的类，用于保存提供该服务的 worker 。
        :param worker_class: 用于表示远程 worker 的类，保存worker的属性和状态。
        :param zap_mechanism: 使用哪一种 zap 验证机制。
        :param zap_addr: zap 身份验证服务的地址。
        :param zap_domain: 需要告诉 zap 服务在哪个域验证。
        """
        self.running = False
        self.loop = get_running_loop()

        # https://zguide.zeromq.org/docs/chapter3/#The-CZMQ-High-Level-API
        self.frontend = self.ctx.socket(z_const.ROUTER, z_aio.Socket)
        self.frontend_task = BoundedDict(None, 10*Settings.ZMQ_HWM, 0.1)
        self.backend = self.ctx.socket(z_const.DEALER, z_aio.Socket)
        self.backend_task = BoundedDict(None, 10*Settings.ZMQ_HWM, 0.1)
        self.backend.bind(backend_url)

        self.heartbeat = min(heartbeat, Settings.MDP_HEARTBEAT)
        self._service_class = service_class
        self._worker_class: Callable[..., RemoteWorker] = worker_class
        # TODO: 本地或远程的 service 和 work 对 broker 应该是同样的。
        self.services: Dict[str, RemoteService] = dict()
        self.workers: Dict[str, RemoteWorker] = dict()
        self.ban_timer_handle: Dict[str, asyncio.TimerHandle] = dict()
        # TODO: 用于跟踪客户端的请求，如果处理请求的后端服务掉线了，向可用的其他后端服务重发。
        self.cache_request = dict()

        self.zap_mechanism: str = ""
        self.zap_socket: Union[z_aio.Socket, None] = None
        self.zap_domain = zap_domain.encode("utf-8")
        self.zap_replies: BoundedDict[str, list] = BoundedDict(
            None, 10*Settings.ZMQ_HWM, 1.5
        )
        if zap_mechanism:
            self.zap_mechanism = zap_mechanism
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

    async def register_worker(self, worker_address):
        """
        添加 worker
        """
        worker_id = hash(worker_address)
        worker = self._worker_class(worker_id, worker_address, self.heartbeat)
        logger.info(f"Register the worker '{worker.identity}:{worker.address}'")
        message = [
            to_bytes(worker_address)
        ]

    async def ban_worker(self, worker: RemoteWorker):
        """
        屏蔽掉指定 worker
        """
        logger.warning(f"Ban the worker '{worker.identity}:{worker.address}'.")
        if worker.identity in self.ban_timer_handle:
            self.ban_timer_handle[worker.identity].cancel()
            self.ban_timer_handle.pop(worker.identity)
        if worker.identity not in self.workers:
            logger.warning(f"The worker '{worker.identity}' has been banned.")
            return True
        self.workers.pop(worker.identity, None)
        if worker.service in self.services:
            self.services[worker.service].remove_worker(worker)
        if worker.is_alive():
            request = [
                to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_DISCONNECT
            ]
            await self.backend.send_multipart(request)
        return True

    def ban_worker_after_expiration(self, worker: RemoteWorker):
        self.ban_timer_handle[worker.identity] = self.loop.call_at(
            worker.expiry, self.ban_worker, worker
        )

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
        (
            empty,
            zap_version,
            request_id,
            status_code,
            status_text,
            user_id,
            metadata
        ) = replies
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

    async def process_replies_from_backend(self, replies):
        logger.debug(f"replies from backend: {replies}")
        (
            worker_addr,
            empty1,
            mdp_worker_version,
            command,
            message
        ) = replies
        if empty1:
            logger.error(f"Not an MDP message.")
            return
        worker_id = hash(worker_addr)
        # MDP 定义的 Worker 命令
        command = command.decode("utf-8")
        if worker_id in self.workers:
            worker = self.workers[worker_id]
        else:
            worker = self._worker_class(
                worker_id, worker_addr, self.heartbeat, ""
            )

        if command == Settings.MDP_COMMAND_READY:
            if not isinstance(message, (bytes, Frame)):
                logger.error(f"message: {replies}")
                logger.error(
                    "This message does not conform to the format of the "
                    "READY command."
                )
                return await self.ban_worker(worker)
            if worker.ready:
                logger.error(f'The worker had already sent the "READY" command')
                return await self.ban_worker(worker)
            service_name = message.decode("utf-8")
            if service_name not in self.services:
                service = self._service_class(
                    service_name, ""
                )
                self.services[service_name] = service
            else:
                service = self.services[service_name]
            worker.ready = True
            service.add_worker(worker)
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_HEARTBEAT:
            if not worker.ready:
                return await self.ban_worker(worker)
            self.ban_timer_handle[worker.identity].cancel()
            worker.expiry = time() + self.heartbeat
            self.ban_worker_after_expiration(worker)

        elif command == Settings.MDP_COMMAND_REPLY:
            if not worker.ready:
                return await self.ban_worker(worker)
            service = self.services[worker.service]
            service.idle_workers.append(worker.identity)
            client_addr, empty, message = message
            reply = [
                client_addr,
                empty,
                Settings.MDP_CLIENT,
                worker.service,
            ]
            if isinstance(message, list):
                reply += message
            else:
                reply.append(message)
            await self.reply_frontend(reply)

        elif command == Settings.MDP_COMMAND_DISCONNECT:
            worker.ready = False
            worker.expiry = time()
            await self.ban_worker(worker)
        else:
            logger.error(f"Invalid command: {command}")

    async def reply_frontend(self, reply: list):
        reply = [to_bytes(frame) for frame in reply]
        await self.frontend.send_multipart(reply)

    async def process_request_from_frontend(self, request):
        if not self.workers:
            logger.debug("no available workers.")
            return False
        logger.debug(f"replies from frontend: {request}")
        (
            client_addr,
            empty,
            mdp_client_version,
            service_name,
            message
        ) = request
        if empty:
            return False

        if self.zap_socket:
            client_id, mechanism, *credentials, request_id, request = \
                message
            zap_result = await self.request_zap(
                client_id, client_addr, request_id, mechanism, credentials
            )
            if not zap_result:
                return False
        else:
            request_id, request = message

        await self.request_backend(
            service_name, client_addr, request_id, request
        )
        return True

    async def request_backend(
        self, service_name, client_addr, request_id, message: list
    ):
        service = self.services[service_name]
        if not service.running:
            reply = [
                client_addr,
                b"",
                Settings.MDP_CLIENT,
                service_name,
                gen_reply(exception=ServiceUnAvailableError(service_name))
            ]
            await self.reply_frontend(reply)
        for worker_id in list(service.idle_workers):
            service.idle_workers.remove(worker_id)
            worker = service.workers[worker_id]
            request = [
                to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_REQUEST,
                client_addr,
                b"",
                request_id
            ] + message
            request = [to_bytes(frame) for frame in request]
            try:
                await self.backend.send_multipart(request)
                return True
            except Exception as e:
                logger.error(
                    f"The request({request}) sent to backend failed.\n"
                    f"error: {e}"
                )
        else:
            logger.error(f'"{service_name}" service has no idle workers.')
            # TODO: 给客户端返回无可用后端的错误，后端是否一次只响应一个请求？或者可配置？
            reply = [
                client_addr,
                b"",
                Settings.MDP_CLIENT,
                service_name,
                gen_reply(exception=BuysWorkersError())
            ]
            await self.reply_frontend(reply)
            return False

    async def send_heartbeat(self):
        for worker in self.workers.values():
            message = [
                to_bytes(worker.address),
                b"",
                Settings.MDP_WORKER,
                Settings.MDP_COMMAND_HEARTBEAT
            ]
            ht = asyncio.create_task(
                self.backend.send_multipart(message)
            )
            k = f"heartbeat_task_{worker.identity}"
            await self.backend_task.aset(k, ht)
            ht.add_done_callback(lambda fut: self.backend_task.pop(k))
        return True

    async def run(self):
        """
        TODO: 客户端使用 DEALER 类型 socket，每个请求都有一个id，
          服务端使用 DEALER 类型 socket, 每个回复都有一个匹配的id。
          客户端需要将回复和请求对应上。
        """
        poller = z_aio.Poller()
        poller.register(self.frontend, z_const.POLLIN)
        poller.register(self.backend, z_const.POLLIN)
        if self.zap_socket:
            poller.register(self.zap_socket, z_const.POLLIN)
        self.running = True

        ft_id = 0
        bt_id = 0
        while 1:
            try:
                if not self.workers:
                    logger.warning("no running workers.")
                    break

                socks = dict(await poller.poll(self.heartbeat))
                for wid, worker in list(self.workers.items()):
                    if not worker.is_alive():
                        await self.ban_worker(worker)
                # TODO: 前后端的接收处理也可以直接放在loop里
                if socks.get(self.backend, None) == z_const.POLLIN:
                    replies = [
                        from_bytes(b)
                        for b in await self.backend.recv_multipart()
                    ]
                    # 后端异步任务，避免等待处理后端回复导致阻塞。
                    bt = asyncio.create_task(
                        self.process_replies_from_backend(replies)
                    )
                    k = f"backend_task_{bt_id}"
                    await self.backend_task.aset(k, bt)
                    bt.add_done_callback(lambda fut: self.backend_task.pop(k))
                    bt_id += 1

                if socks.get(self.frontend, None) == z_const.POLLIN:
                    replies = [
                        from_bytes(b)
                        for b in await self.frontend.recv_multipart()
                    ]
                    # 同后端任务处理。
                    ft = asyncio.create_task(
                        self.process_request_from_frontend(replies)
                    )
                    await self.frontend_task.aset(f"frontend_task_{ft_id}", ft)
                    ft.add_done_callback(
                        lambda fut: self.frontend_task.pop(f"frontend_task_{ft_id}")
                    )
                    ft_id += 1

                if socks.get(self.zap_socket, None) == z_const.POLLIN:
                    replies = await self.zap_socket.recv_multipart()
                    await self.process_replies_from_zap(replies)

                await self.send_heartbeat()
            except Exception:
                logger.error(format_exception(*sys.exc_info()))
                break

        await self.disconnect_all()
        poller.unregister(self.frontend)
        poller.unregister(self.backend)
        if self.zap_socket:
            poller.unregister(self.zap_socket)
        logger.info("proxy loop exit.")
        return

    async def disconnect_all(self):
        # TODO: 需要通知客户端吗？
        logger.warning("Disconnected and all the workers.")
        ban_result = await asyncio.gather(
            [
                self.ban_worker(work)
                for work in self.workers.values()
            ],
            return_exceptions=True
        )
        logger.warning(f"Disconnected execution result:\n{pformat(ban_result)}")


class Client(object):

    def __init__(
        self,
        context: Context = None,
        heartbeat: float = Settings.MDP_HEARTBEAT,
        timeout: int = None,
        msg_queue_max: int = Settings.MESSAGE_QUEUE_MAX,
        zap_mechanism: str = Settings.ZAP_MECHANISM_NULL,
        credential_frames: list = None
    ):
        """
        客户端，
        TODO：连接远程的 MDP 服务，连接后发送一条查询有哪些服务的消息，
          然后将包含的服务添加为客户端关联的服务实例，链式调用服务包含的方法。
        TODO: 对于没有收到回复的请求记录并保存下来，可以设置重试次数来重新请求。
        TODO: 从rpc获取的所有函数，在客户端这里都用这个生成对应的本地函数。
        :param context: zmq 的上下文实例，为空则创建。
        :param heartbeat: 维持连接的心跳消息间隔，默认为 MDP_HEARTBEAT。
        :param timeout: 连接超时时间，默认为 CLIENT_TIMEOUT。
        :param msg_queue_max: 保存接收到的消息的队列的大小，默认为 MSG_QUEUE_MAX。
        :param zap_mechanism: zap 的验证机制。
        :param credential_frames: zap 的凭据帧列表，不填则为空列表。
        """
        self.loop = get_running_loop()
        if not context:
            self.ctx = Context()
        else:
            self.ctx = context
        self.socket = self.ctx.socket(z_const.DEALER, z_aio.Socket)

        self.heartbeat = heartbeat
        if timeout:
            self.timeout = timeout * 1000
        else:
            self.timeout = Settings.CLIENT_TIMEOUT * 1000

        self.msg_queue_max = msg_queue_max
        self.replies: BoundedDict[str, list] = BoundedDict(
            None, 3*Settings.ZMQ_HWM, 1.5
        )

        self.identity = f"gc-{uuid4()}"
        self.request_id = 0
        if credential_frames is None:
            credential_frames = list()
        if zap_mechanism == Settings.ZAP_MECHANISM_NULL:
            assert len(credential_frames) == 0, \
                AttributeError(
                    f'The "{Settings.ZAP_MECHANISM_NULL}" mechanism '
                    f'should not have credential frames.'
                )
        elif zap_mechanism == Settings.ZAP_MECHANISM_PLAIN:
            assert len(credential_frames) == 2, \
                AttributeError(
                    f'The "{Settings.ZAP_MECHANISM_PLAIN}" mechanism '
                    f'should have tow credential frames: '
                    f'a username and a password.'
                )
        elif zap_mechanism == Settings.ZAP_MECHANISM_CURVE:
            raise RuntimeError(
                f'The "{Settings.ZAP_MECHANISM_CURVE}" mechanism is not implemented yet.'
            )
        else:
            raise ValueError(
                f'mechanism can only be '
                f'"{Settings.ZAP_MECHANISM_NULL}" or '
                f'"{Settings.ZAP_MECHANISM_PLAIN}" or '
                f'"{Settings.ZAP_MECHANISM_CURVE}"'
            )
        self.zap_frames = [self.identity, zap_mechanism] + credential_frames

    def connect(self, addr):
        self.socket.connect(addr)
    
    def disconnect(self, addr):
        self.socket.disconnect(addr)

    async def _send(self, request: list):
        request = [to_bytes(frame) for frame in request]
        await self.socket.send_multipart(request)

    async def _recv(self):
        poller = z_aio.Poller()
        poller.register(self.socket, z_const.POLLIN)
        while 1:
            socks = poller.poll(self.timeout)
            if socks:
                empty, mdp_client_version, service_name, reply = \
                    await self.socket.recv_multipart()
                logger.debug(
                    f"Reply: \n"
                    f"frame 0: {empty},\n"
                    f"frame 1: {mdp_client_version},\n"
                    f"frame 2: {service_name},\n"
                    f"frame 3: {reply}"
                )
                if not empty or mdp_client_version != Settings.MDP_CLIENT:
                    continue
                reply = msg_load(reply)
                for request_id, result in reply.items():
                    await self.replies.aset(request_id, result)

    async def hit_road(
        self, service_name: str, func_name: str, *args, **kwargs
    ) -> str:
        """
        调用指定的rpc服务。
        :param service_name: 要访问的服务的名字
        :param func_name: 要调用该服务的函数的名字
        :param args: 要调用的该函数要传递的位置参数
        :param kwargs: 要调用的该函数要传递的关键字参数
        """
        self.request_id += 1
        request_id = f"{self.identity}-{self.request_id}"
        body = msg_dump(
            {
                func_name: {
                    "args": args,
                    "kwargs": kwargs
                }
            }
        )
        body = [request_id, body]
        request = ["", Settings.MDP_CLIENT, service_name]
        request += self.zap_frames
        request += body
        await self._send(request=request)
        return request_id

    async def return_from_journey(
        self, service_name: str, func_name: str,
        *args, **kwargs
    ):
        """
        获取 rpc 返回结果，如果有异常就重新抛出异常，
        :return:
        """
        request_id = await self.hit_road(
            service_name, func_name, *args, **kwargs
        )
        # 使用可等待异步字典，不需要单独写一个等待方法了。
        result: Dict = await self.replies.aget(request_id, ignore=False)
        self.replies.pop(request_id)
        # 返回结果则是常规字典。
        return_obj = result["return"]
        exception = result["exception"]
        if exception:
            raise exception
        return return_obj

    async def run(self):
        recv_task = self.loop.create_task(self._recv())
