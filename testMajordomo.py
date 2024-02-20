# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
# import uvloop
import secrets
import sys
from logging import getLogger
from time import time
from traceback import format_exception

from gaterpc.global_settings import Settings
from gaterpc.core import AsyncZAPService, Worker, Service, AMajordomo, Client
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


async def test():
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 0.01
    Settings.DEBUG = True
    # Settings.EVENT_LOOP_POLICY = uvloop.EventLoopPolicy()
    Settings.setup()
    zap = AsyncZAPService()
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )
    zap.start()
    gr_majordomo = AMajordomo(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
        zap_addr=Settings.ZAP_ADDR
    )
    gr_majordomo.bind("tcp://127.0.0.1:777")
    gr_majordomo.run()
    gr = Service()
    gr_worker = gr.create_worker(
        GRWorker,
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
    gr_worker.run()
    await asyncio.sleep(5)
    gr_cli = Client(
        broker_addr="tcp://127.0.0.1:777",
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    await asyncio.sleep(5)
    logger.info(await gr_cli.GateRPC.get_interfaces())
    logger.info(gr_cli._remote_services)
    logger.info(await gr_cli.Gate.query_service("GateRPC"))
    i = 100
    logger.info("start test")
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
        logger.debug(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        logger.debug(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        await asyncio.sleep(1)
        logger.debug(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        logger.debug(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        gr_cli.close()
        await gr_worker.stop()
        gr_majordomo.stop()
        zap.stop()


if __name__ == "__main__":
    asyncio.run(test(), debug=True)
