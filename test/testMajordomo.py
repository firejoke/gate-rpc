# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import hashlib
# import uvloop
import secrets
import sys
from logging import getLogger
from pathlib import Path
from traceback import format_exception
from typing import Optional, Union
from uuid import uuid4


base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.global_settings import Settings
from gaterpc.core import (
    AsyncZAPService, Context, Worker, Service, AMajordomo,
)
from gaterpc.utils import (
    HugeData, interface, msg_pack, run_in_executor,
    to_bytes,
)
import testSettings


Settings.configure("USER_SETTINGS", testSettings)
logger = getLogger("commands")


class TAMajordomo(AMajordomo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clients.add(self.identity)
        self.rw_queue = asyncio.Queue()
        self.rw_task: Optional[asyncio.Task] = None
        self.rw_replies = dict()

    async def request_worker(self):
        await asyncio.sleep(3)
        try:
            rpc_service = self.services[Settings.SERVICE_DEFAULT_NAME]
        except KeyError:
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
                worker = await rpc_service.acquire_idle_worker()
            except asyncio.TimeoutError:
                continue
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
            option = (self.identity, b"", request_id)
            # 需要顺序执行的，就顺序等待发送
            # await self.send_to_backend(
            #     worker.identity, Settings.MDP_COMMAND_REQUEST,
            #     option, body
            # )
            # 不需要顺序执行，更在意并发，则创建为发送任务
            self.backend_tasks.add(
                t := self._loop.create_task(
                    self.send_to_backend(
                            worker.identity, Settings.MDP_COMMAND_REQUEST,
                            option, body
                        )
                )
            )
            t.add_done_callback(self.cleanup_backend_task)
            if log["gtid"] == 100000:
                logger.debug(f"sent use time: {self._loop.time() - st}")
            self.rw_replies[request_id] = self._loop.create_future()
            i += 1

    rw_replies_num = 0

    async def reply_frontend(
        self, client_id: bytes,
        service_name: Union[str, bytes],
        request_id: Union[str, bytes],
        *body
    ):
        if client_id == self.identity:
            if request_id in self.rw_replies:
                self.rw_replies.pop(request_id).set_result(body)
                if not self.rw_replies_num:
                    logger.debug(f"first replies: {self._loop.time()}")
                self.rw_replies_num += 1
                if not self.rw_replies_num % 10000:
                    logger.debug(
                        f"worker replies number: {self.rw_replies_num}, "
                        f"time: {self._loop.time()}"
                    )
        else:
            await super().reply_frontend(
                client_id, service_name, request_id, *body
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
        if not self._broker_task:
            self._broker_task = self._loop.create_task(self._broker_loop())
        if not self.rw_task:
            self.rw_task = self._loop.create_task(self.request_worker())

    def stop(self):
        if self.rw_task:
            if not self.rw_task.cancelled():
                self.rw_task.cancel()
            self.rw_task = None
        if self._broker_task:
            if not self._broker_task.cancelled():
                self._broker_task.cancel()
            self.close()
            self._broker_task = None


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


async def majordomo(
    frontend=None, backend=None, bind_gate=None, connect_gate=None
):
    Settings.setup()
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    if backend:
        Settings.WORKER_ADDR = backend
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    ctx = Context()
    zipc = Settings.ZAP_ADDR
    # zipc = f"ipc://{zipc.as_posix()}"
    logger.info(f"zap ipc addr: {zipc}")
    zap = AsyncZAPService(addr=zipc)
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )

    gr_majordomo = TAMajordomo(
        # context=ctx,
        gate_zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        gate_zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    gr_majordomo.bind_backend()
    gr_majordomo.bind_frontend(frontend)
    if bind_gate:
        gr_majordomo.bind_gate(bind_gate)
    await asyncio.sleep(1)
    logger.info("start test")
    try:
        zap.start()
        gr_majordomo.connect_zap(zap_addr=zipc)
        gr_majordomo.run()
        if connect_gate:
            try:
                await gr_majordomo.connect_gate(connect_gate)
            except Exception as e:
                logger.error(e)
                raise e
        await asyncio.sleep(5)
        # await gr_majordomo.test_rw()
        await gr_majordomo._broker_task
    finally:
        gr_majordomo.stop()
        logger.info(f"request_zap.cache_info: {gr_majordomo.zap_cache}")
        zap.stop()


def test(frontend=None, backend=None, bind_gate=None, connect_gate=None):
    asyncio.run(
        majordomo(frontend, backend, bind_gate, connect_gate)
    )


if __name__ == "__main__":
    argv = sys.argv[1:]
    if not argv:
        argv = [
            "ipc:///tmp/gate-rpc/run/c1",
            Settings.WORKER_ADDR, None, None
        ]
    if len(argv) == 1:
        argv.extend([Settings.WORKER_ADDR, None, None])
    elif len(argv) == 2:
        argv.extend([None, None])
    elif len(argv) == 3:
        argv.append(None)
    print(argv)
    test(*argv)
