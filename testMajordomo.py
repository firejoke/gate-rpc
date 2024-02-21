# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import concurrent.futures
# import uvloop
import secrets
import sys
from logging import getLogger
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


async def client():
    gr_cli = Client(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )

    gr_cli.connect("tcp://127.0.0.1:777")
    await asyncio.sleep(5)
    logger.info(await gr_cli.GateRPC.get_interfaces())
    logger.info(gr_cli._remote_services)
    logger.info(await gr_cli.Gate.query_service("GateRPC"))
    i = 100
    try:
        while i:
            result = await gr_cli.test("a", "b", "c", time=time())
            logger.info("=====================================================")
            logger.info(f"result of test: {result}")
            logger.info("=====================================================")
            result = await gr_cli.atest("d", "e", "f", time=time())
            logger.info("=====================================================")
            logger.info(f"result of atest: {result}")
            logger.info("=====================================================")
            i -= 1
        rw_i = 0
        async for i in await gr_cli.test_agenerator(10):
            logger.info("=====================================================")
            logger.info(f"get i for agen: {i}")
            logger.info("=====================================================")
            if i != rw_i:
                raise RuntimeError
            rw_i += 1
        dd = await gr_cli.test_huge_data()
        logger.info("=====================================================")
        logger.info(f"length of dd: {len(dd)}")
        logger.info(f"dd is bs: {dd == s}")
        logger.info("=====================================================")
        logger.info("check heartbeat")
        await asyncio.sleep(5)
        # await gr_majordomo._broker_task
    except Exception as e:
        logger.info("*****************************************************")
        for line in format_exception(*sys.exc_info()):
            logger.error(line)
        logger.info("*****************************************************")
        raise e
    finally:
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        gr_cli.close()


def start_client():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(client())


async def test():
    loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    Settings.DEBUG = True
    Settings.ZAP_ADDR = "inproc://zeromq.zap.01"
    Settings.ZAP_REPLY_TIMEOUT = 10.0
    # Settings.EVENT_LOOP_POLICY = uvloop.EventLoopPolicy()
    Settings.setup()
    ctx = Context()
    # zap_thread = Thread(target=start_zap, args=(ctx,))
    zap = AsyncZAPService(context=ctx)
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )

    gr_majordomo = AMajordomo(context=ctx)
    gr_majordomo.bind("tcp://127.0.0.1:777")
    await asyncio.sleep(5)
    logger.info("start test")
    gr = Service()
    gr_worker = gr.create_worker(
        GRWorker,
        context=ctx,
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
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
        gr_majordomo.connect_zap(
            zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
            zap_addr=Settings.ZAP_ADDR
        )
        gr_majordomo.run()
        gr_worker.run()
        await asyncio.sleep(3)
        await client()
        # await asyncio.sleep(120)
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
        zap.stop()


if __name__ == "__main__":
    # client_thread = Thread(target=start_client)
    # client_thread.start()
    asyncio.run(test(), debug=True)
