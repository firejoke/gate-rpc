# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import sys
from time import time
from traceback import format_exception

from gaterpc.global_settings import Settings
from gaterpc.core import IOWorker, Service, AMajordomo, Client


class SRWorker(IOWorker):
    pass


class SRService(Service):
    pass


class SRMajordomo(AMajordomo):
    pass


class SRClient(Client):
    pass


async def test():
    Settings.setup()
    sr_majordomo = SRMajordomo(backend_addr="tcp://127.0.0.1:5555")
    sr_majordomo.bind("tcp://127.0.0.1:777")
    sr_majordomo.run()
    sr = SRService(name="SRkv")
    sr_worker = sr.create_worker(
        "io", SRWorker, "tcp://127.0.0.1:5555"
    )
    print(sr_worker.service)
    if sr_worker.service is not sr:
        return
    print(sr_worker.interfaces)
    sr_worker.run()
    await asyncio.sleep(5)
    sr_cli = SRClient("tcp://127.0.0.1:777")
    i = 100
    print("start test")
    try:
        while i:
            result = sr_cli.SRkv.atest("a", "b", "c", time=time())
            print(await result)
            i -= 1
    except Exception:
        for line in format_exception(*sys.exc_info()):
            print(line)
        sr_cli.disconnect()
        await sr_worker.stop()
        sr_majordomo.stop()


if __name__ == "__main__":
    asyncio.run(test())
