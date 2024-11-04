# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/23 13:51
import asyncio
import hashlib
import secrets
import sys
from ctypes import c_wchar_p
from logging import getLogger
from pathlib import Path

import zmq.auth
import zmq.constants as z_const
from zmq.backend import curve_keypair


base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.global_settings import Settings
from gaterpc.core import (
    AsyncZAPService, Context, Worker, Service, AMajordomo,
)
from gaterpc.utils import (
    HugeData, UnixEPollEventLoopPolicy, interface, msg_pack, run_in_executor,
    to_bytes,
)
import testSettings


curve_dir = Path(__file__).parent.joinpath("curvekey/")
if curve_dir.exists():
    g_public, _ = zmq.auth.load_certificate(
        curve_dir.joinpath("gate.key")
    )
    # cw_public, cw_secret = zmq.auth.load_certificate(
    #     curve_dir.joinpath("cw.key_secret")
    # )
    cw_public, cw_secret = curve_keypair()
else:
    g_public = b""
    cw_public = cw_secret = b""

Settings.configure("USER_SETTINGS", testSettings)
Settings.DEBUG = 1
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
        return {
            "name": "async atest",
            "args": args,
            "kwargs": kwargs,
            "loop_time": self._loop.time()
        }

    @interface("none")
    def test(self, *args, **kwargs):
        return {
            "name": "test",
            "args": args,
            "kwargs": kwargs,
            "loop_time": self._loop.time()
        }

    @interface("thread")
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
    async def test_huge_data(self):
        # logger.info(f"s sha256: {s_256}")
        try:
            data = await run_in_executor(
                self._loop, self.process_executor, 
                msg_pack, s
            )
            logger.info(f"data len: {len(data)}, sha256: {s_256}")
            hd = HugeData(
            Settings.HUGE_DATA_END_TAG,
            Settings.HUGE_DATA_EXCEPT_TAG,
            data=data
        )
            return hd
        except Exception as e:
            logger.error(e)

    @interface
    async def emit(self, log):
        try:
            lid = log["gtid"]
            return lid
        except Exception as e:
            logger.error(e)
            raise


async def worker(backend_addr=None):
    Settings.setup()
    if g_public:
        print(
            f"g_public: {g_public}\n"
            f"cw_public: {cw_public}, cw_secret: {cw_secret}"
        )
        Settings.ZMQ_SOCK.update({
            z_const.CURVE_SECRETKEY: cw_secret,
            z_const.CURVE_PUBLICKEY: cw_public,
            z_const.CURVE_SERVERKEY: g_public,
        })
    # loop = asyncio.get_event_loop()
    # loop.slow_callback_duration = 0.01
    if backend_addr:
        Settings.WORKER_ADDR = backend_addr
    ctx = Context()
    gr = Service()
    gr_worker = gr.create_worker(
        GRWorker,
        context=ctx,
        # zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        # zap_credentials=(
        #     Settings.ZAP_PLAIN_DEFAULT_USER,
        #     Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        # ),
    )
    logger.info(gr_worker.service)
    if gr_worker.service is not gr:
        return False
    logger.info(gr_worker.interfaces)
    try:
        gr_worker._reconnect = True
        gr_worker.run()
        while 1:
            if gr_worker._recv_task:
                if gr_worker._recv_task.done():
                    break
                await gr_worker._recv_task
    finally:
        logger.info(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        await asyncio.sleep(1)
        logger.info(
            f"the length of worker's requests: {len(gr_worker.requests)}"
        )
        gr_worker.stop()


def test(backend_addr=None):
    # asyncio.set_event_loop_policy(UnixEPollEventLoopPolicy())
    asyncio.run(worker(backend_addr))


if __name__ == '__main__':
    if len(argv := sys.argv) > 1:
        test(argv[1])
    else:
        test()
