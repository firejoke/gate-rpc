# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import secrets
import sys
from collections.abc import AsyncGenerator, Generator
from logging import getLogger
from time import time
from traceback import format_exception

from gaterpc.global_settings import Settings
from gaterpc.core import Worker, Service, AMajordomo, Client
from gaterpc.utils import interface


logger = getLogger("commands")
s = ""
i = 10000
while i:
    s += secrets.token_hex()
    i -= 1


class GRWorker(Worker):
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
    Settings.DEBUG = True
    Settings.setup()
    gr_majordomo = AMajordomo(backend_addr="tcp://127.0.0.1:5555")
    gr_majordomo.bind("tcp://127.0.0.1:777")
    gr_majordomo.run()
    gr = Service()
    gr_worker = gr.create_worker(
       GRWorker, "tcp://127.0.0.1:5555"
    )
    logger.info(gr_worker.service)
    if gr_worker.service is not gr:
        return False
    logger.info(gr_worker.interfaces)
    gr_worker.run()
    await asyncio.sleep(5)
    gr_cli = Client(broker_addr="tcp://127.0.0.1:777")
    i = 100
    logger.info("start test")
    try:
        while i:
            result = await gr_cli.test("a", "b", "c", time=time())
            logger.info(f"result of test: {result}")
            result = await gr_cli.atest("d", "e", "f", time=time())
            logger.info(f"result of atest: {result}")
            i -= 1
        async for i in await gr_cli.test_agenerator(10):
            logger.info(f"get i for agen: {i}")
        dd = await gr_cli.test_huge_data()
        logger.info(f"length of dd: {len(dd)}")
        logger.info(f"dd is bs: {dd == s}")
    except Exception as e:
        for line in format_exception(*sys.exc_info()):
            logger.error(line)
        gr_cli.close()
        await gr_worker.stop()
        gr_majordomo.stop()
        raise e


if __name__ == "__main__":
    asyncio.run(test(), debug=True)
