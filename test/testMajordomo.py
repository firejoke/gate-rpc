# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
# import uvloop
import sys
from logging import getLogger
from pathlib import Path
from traceback import format_exception
from typing import Optional, Union
from uuid import uuid4

import zmq.auth
import zmq.constants as z_const



base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.global_settings import Settings
from gaterpc.core import (
    AsyncZAPService, Context, Worker, Service, AMajordomo, Gate
)
from gaterpc.utils import (
    HugeData, UnixEPollEventLoopPolicy, interface, msg_pack, msg_unpack,
    run_in_executor,
    to_bytes,
)
from gaterpc.exceptions import BusyWorkerError, ServiceUnAvailableError
import testSettings


curve_dir = Path(__file__).parent.joinpath("curvekey/")
if curve_dir.exists():
    g_public, g_secret = zmq.auth.load_certificate(
        curve_dir.joinpath("gate.key_secret")
    )
    print(f"g_public: {g_public}, g_secret: {g_secret}")
else:
    g_public = g_secret = b""

Settings.configure("USER_SETTINGS", testSettings)
logger = getLogger("commands")


class Mixin:

    rw_replies_num = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rw_queue = asyncio.Queue()

    async def request_worker(self):
        await asyncio.sleep(3)
        if Settings.SERVICE_DEFAULT_NAME not in self.services:
            logger.error("RPC service don't running.")
            await asyncio.sleep(1)
            self.rw_task = self._loop.create_task(self.request_worker())
            return
        st = 0
        i = 0
        while 1:
            if i > 100000:
                break
            try:
                if not st:
                    st = self._loop.time()
                    logger.debug(f"start time: {st}")
                log = {
                    "gtid": i + 1,
                    "action": "update",
                    "key": "tttxxxhhh",
                    "value": {
                        "name": "hostname",
                        "ip": "1.1.1.1",
                        "stat": "running" if i % 2 else "stop",
                        "remote_hosts": ["hostname1", "hostname2", "hostname3"]
                    }
                }
                request_id = to_bytes(uuid4().hex)
                body = (
                    msg_pack("emit"),
                    msg_pack((log,))
                )
                await self.request_backend(
                    Settings.SERVICE_DEFAULT_NAME,
                    self.identity,
                    request_id,
                    body
                )
                if not log["gtid"] % 10000:
                    logger.info(f"sent {log['gtid']}")
                if log["gtid"] == 100000:
                    logger.info(f"sent use time: {self._loop.time() - st}")
                i += 1
                self.create_internal_task(
                    self.process_rw_replies, func_args=(request_id,)
                )
            except (ServiceUnAvailableError, BusyWorkerError):
                continue

            # 要注意并发太多时，对套接字类型的选择和缓冲区的配置，同时适时的让出io
            # 可以适当提高Settings里的ZMQ_SOCk配置里的 z_const.HWM，
            # sysctl -w net.core.wmem_default=33554432
            # sysctl -w net.core.rmem_default=33554432
            # sysctl -w net.core.wmem_max=67108864
            # sysctl -w net.core.rmem_max=67108864

            # await asyncio.sleep(0)

    async def process_rw_replies(
        self, request_id: Union[str, bytes],
    ):
        body = await self.internal_requests[request_id]
        # logger.debug(f"result: {msg_unpack(body[0])}")
        self.internal_requests.pop(request_id)
        if not self.rw_replies_num:
            logger.debug(f"first replies: {self._loop.time()}")
        self.rw_replies_num += 1
        if not self.rw_replies_num % 10000:
            logger.debug(
                f"worker replies number: {self.rw_replies_num}, "
                f"time: {self._loop.time()}"
            )

    async def test_rw(self):
        await asyncio.sleep(5)
        logger.info("start test rw")
        try:
            st = self._loop.time()
            for i in range(100000):
                log = {
                    "gtid": i + 1,
                    "action": "update",
                    "key": "tttxxxhhh",
                    "value": {
                        "name": "hostname",
                        "ip": "1.1.1.1",
                        "stat": "running" if i % 2 else "stop",
                        "remote_hosts": ["hostname1", "hostname2", "hostname3"]
                    }
                }
                await self.rw_queue.put(log)
                await asyncio.sleep(0)
            et = self._loop.time()
            logger.info(f"put use time: {et - st}")
        except Exception as e:
            logger.info("*****************************************************")
            for line in format_exception(*sys.exc_info()):
                logger.error(line)
            logger.info("*****************************************************")
            raise e

    def run(self):
        super().run()
        self.create_internal_task(self.request_worker, name="rw_task")

    def stop(self):
        super().stop()


class TAMajordomo(Mixin, AMajordomo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TGagte(Mixin, Gate):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Mixin.__init__(self, *args, **kwargs)


async def zap_server(ctx):
    zap = AsyncZAPService(context=ctx)
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )
    try:
        zap.start()
        await zap._recv_task
    finally:
        zap.stop()


def start_zap(ctx):
    loop = asyncio.new_event_loop()
    loop.slow_callback_duration = 0.01
    loop.run_until_complete(zap_server(ctx))


async def majordomo(frontend=None, backend=None):
    Settings.setup()
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    if backend:
        Settings.WORKER_ADDR = backend
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    ctx = Context()
    # zipc = Settings.ZAP_ADDR
    # logger.info(f"zap ipc addr: {zipc}")
    # zap = AsyncZAPService(addr=zipc)
    # zap.configure_plain(
    #     Settings.ZAP_DEFAULT_DOMAIN,
    #     {
    #         Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
    #     }
    # )

    gr_majordomo = TAMajordomo(
        context=ctx,
    )
    if g_secret:
        gr_majordomo.bind_backend(
            sock_opt={
                z_const.CURVE_SECRETKEY: g_secret,
                # z_const.CURVE_PUBLICKEY: g_public,
                z_const.CURVE_SERVER: True,
            }
        )
    else:
        gr_majordomo.bind_backend()
    if frontend:
        if g_secret:
            gr_majordomo.bind_frontend(
                frontend,
                sock_opt={
                    z_const.CURVE_SECRETKEY: g_secret,
                    # z_const.CURVE_PUBLICKEY: g_public,
                    z_const.CURVE_SERVER: True,
                }
            )
        else:
            gr_majordomo.bind_frontend(frontend)
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        # zap.start()
        # gr_majordomo.connect_zap(zap_addr=zipc)
        gr_majordomo.run()
        await asyncio.sleep(5)
        # await gr_majordomo.test_rw()
        await gr_majordomo._broker_task
    finally:
        gr_majordomo.stop()
        logger.info(f"request_zap.cache_info: {gr_majordomo.zap_cache}")
        # zap.stop()


async def tgate(
    gate_port, mcast_port=None, frontend=None, backend=None
):
    if g_secret:
        Settings.GATE_CURVE_KEY = g_secret
        Settings.GATE_CURVE_PUBKEY = g_public
    Settings.setup()
    if backend:
        Settings.WORKER_ADDR = backend
    if mcast_port:
        Settings.GATE_MULTICAST_PORT = mcast_port
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    ctx = Context()
    gate = TGagte(gate_port, context=ctx)
    if g_secret:
        gate.bind_backend(
            sock_opt={
                z_const.CURVE_SECRETKEY: g_secret,
                # z_const.CURVE_PUBLICKEY: g_public,
                z_const.CURVE_SERVER: True,
            }
        )
    else:
        gate.bind_backend()
    if frontend:
        if g_secret:
            gate.bind_frontend(
                frontend,
                sock_opt={
                    z_const.CURVE_SECRETKEY: g_secret,
                    # z_const.CURVE_PUBLICKEY: g_public,
                    z_const.CURVE_SERVER: True,
                }
            )
        else:
            gate.bind_frontend(frontend)
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        gate.run()
        await asyncio.sleep(5)
        await gate._broker_task
    finally:
        gate.stop()


def test(
    frontend="ipc:///tmp/gate-rpc/run/c1",
    backend=None, gate_port=None, mcast_port=None
):
    print(f"frontend: {frontend}, backend: {backend}")
    # asyncio.set_event_loop_policy(UnixEPollEventLoopPolicy())
    if gate_port:
        gate_port = int(gate_port)
        print(f"gate_port: {gate_port}, broadcast_port: {mcast_port}")
        asyncio.run(
            tgate(
                gate_port, mcast_port=mcast_port,
                frontend=frontend, backend=backend
            )
        )
    else:
        asyncio.run(
            majordomo(frontend, backend)
        )


if __name__ == "__main__":
    argv = sys.argv[1:]
    test(*argv)
