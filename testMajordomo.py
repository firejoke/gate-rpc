# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import concurrent.futures
import hashlib
# import uvloop
import secrets
import sys
from copy import copy
from logging import getLogger
from random import randint
from threading import Thread
from time import time
from traceback import format_exception

from gaterpc.global_settings import Settings
from gaterpc.core import (
    AsyncZAPService, Context, Worker, Service, AMajordomo,
    Client,
)
from gaterpc.utils import interface


logger = getLogger("commands")
s = ""
i = 10000
while i:
    s += secrets.token_hex()
    i -= 1

s_256 = hashlib.sha256(s.encode("utf-8")).hexdigest()


class GRWorker(Worker):
    @interface
    async def aconcurrency(self, *args, **kwargs):
        return b""

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

    @interface
    def test_generator(self, maximum: int):
        i = 0
        while i < maximum:
            yield i
            i += 1

    @interface
    async def test_agenerator(self, maximum: int):
        i = 0
        while i < maximum:
            await asyncio.sleep(0.1)
            yield i
            i += 1

    @interface
    def test_huge_data(self):
        logger.info(f"s sha256: {s_256}")
        return s


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


async def test(frontend=None, backend=None, bind_gate=None, connect_gate=None):
    loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    Settings.DEBUG = True
    Settings.WORKER_ADDR = backend
    # Settings.ZAP_ADDR = f"{backend}.zap"
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    # Settings.EVENT_LOOP_POLICY = uvloop.EventLoopPolicy()
    Settings.setup()
    ctx = Context()
    # zap_thread = Thread(target=start_zap, args=(ctx,))
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

    gr_majordomo = AMajordomo(
        context=ctx,
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
    await asyncio.sleep(5)
    logger.info("start test")
    gr = Service()
    gr_worker = gr.create_worker(
        GRWorker,
        context=ctx,
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    logger.info(gr_worker.service)
    if gr_worker.service is not gr:
        return False
    logger.info(gr_worker.interfaces)
    try:
        zap.start()
        await gr_majordomo.connect_zap(zap_addr=zipc)
        gr_majordomo.run()
        gr_worker._reconnect = True
        gr_worker.run()
        await asyncio.sleep(2)
        if connect_gate:
            try:
                await gr_majordomo.connect_gate(connect_gate)
            except Exception as e:
                logger.error(e)
                raise e
        await asyncio.sleep(5)
        # gr_worker._recv_task.cancel()
        await gr_majordomo._broker_task
    finally:
        logger.info(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        await asyncio.sleep(1)
        logger.info(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        gr_worker.stop()
        gr_majordomo.stop()
        logger.info(f"request_zap.cache_info: {gr_majordomo.zap_cache}")
        zap.stop()


if __name__ == "__main__":
    # client_thread = Thread(target=start_client)
    # client_thread.start()
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
    asyncio.run(test(*argv), debug=True)
