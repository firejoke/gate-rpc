# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/2/23 15:55
import asyncio
import hashlib
import inspect
import sys
from logging import getLogger
from pathlib import Path
from traceback import format_exception

base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.core import Client
from gaterpc.global_settings import Settings
from gaterpc.utils import check_socket_addr

import testSettings


Settings.configure("USER_SETTINGS", testSettings)
logger = getLogger("commands")


test_t_set = set()


def cleanup_t(t: asyncio.Task):
    test_t_set.remove(t)
    try:
        if t.exception():
            logger.debug(f"{t.get_name()}.exception: {t.exception()}")
    except asyncio.CancelledError:
        pass


async def client(frontend_addr):
    loop = asyncio.get_running_loop()
    gr_cli = Client(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    await gr_cli.connect(check_socket_addr(frontend_addr))
    await asyncio.sleep(5)
    logger.info(await gr_cli.GateRPC.get_interfaces())
    logger.info(gr_cli._remote_services)
    logger.info(await gr_cli.Gate.query_service("GateRPC"))
    try:
        i = 10000
        st = loop.time()
        while i or test_t_set:
            if i:
                co = gr_cli.test("a", "b", "c", time=gr_cli._loop.time())
                test_t_set.add(
                    t := loop.create_task(co)
                )
                t.add_done_callback(cleanup_t)
                # logger.info("=====================================================")
                # logger.info(f"result of test: {result}")
                # logger.info("=====================================================")
                i -= 1
                if not i:
                    logger.info(f"creat task use time: {loop.time() - st}")
            await asyncio.sleep(0)
        logger.info(f"func test use time: {loop.time() - st}")

        i = 10000
        st = loop.time()
        while i or test_t_set:
            if i:
                co = gr_cli.atest("d", "e", "f", time=gr_cli._loop.time())
                test_t_set.add(
                    t := loop.create_task(co)
                )
                t.add_done_callback(cleanup_t)
                # logger.info("=====================================================")
                # logger.info(f"result of atest: {result}")
                # logger.info("=====================================================")
                i -= 1
                if not i:
                    logger.info(f"creat task use time: {loop.time() - st}")
            await asyncio.sleep(0)
        logger.info(f"func atest use time: {loop.time() - st}")

        rw_i = 0
        st = loop.time()
        async for i in await gr_cli.test_agenerator(10):
            # logger.info("=====================================================")
            # logger.info(f"get i for agen: {i}")
            # logger.info("=====================================================")
            if i != rw_i:
                raise RuntimeError
            rw_i += 1
            await asyncio.sleep(0)
        print(f"rw_i: {rw_i}, use time: {loop.time() - st}")
        st = loop.time()
        dd = await gr_cli.test_huge_data()
        logger.info(f"test_huge_data use time: {loop.time() - st}")
        logger.info("=====================================================")
        logger.info(f"length of dd: {len(dd)}")
        logger.info(
            f"dd sha256: {hashlib.sha256(dd.encode('utf-8')).hexdigest()}"
        )
        logger.info("=====================================================")
        # logger.info("check heartbeat")
        # await gr_cli._recv_task
    except Exception as e:
        logger.info("*****************************************************")
        for line in format_exception(*sys.exc_info()):
            logger.error(line)
        logger.info("*****************************************************")
    finally:
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        logger.info(
            f"the length of client's replies: {len(gr_cli.replies)}"
        )
        gr_cli.close()


async def test(frontend_addr):
    Settings.DEBUG = True
    Settings.setup()
    await client(frontend_addr)


if __name__ == '__main__':
    try:
        frontend_addr = sys.argv[1]
        asyncio.run(test(frontend_addr))
    except IndexError:
        print("require frontend_addr.")
