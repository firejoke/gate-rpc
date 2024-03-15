# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/2/23 15:55
import asyncio
import hashlib
import sys
from logging import getLogger
from time import time
from traceback import format_exception

from gaterpc.core import Client, Context
from gaterpc.global_settings import Settings
from gaterpc.utils import check_socket_addr


logger = getLogger("commands")


async def client(frontend_addr):
    gr_cli = Client(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN.decode("utf-8"),
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )

    gr_cli.connect(check_socket_addr(frontend_addr))
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
        raise e
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
        asyncio.run(test(frontend_addr), debug=True)
    except IndexError:
        print("require frontend_addr.")
