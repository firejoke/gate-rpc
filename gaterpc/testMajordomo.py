# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/12/29 9:12
import asyncio
import sys
from time import time
from traceback import format_exception

from gaterpc.global_settings import Settings
from gaterpc.core import IOWorker, Service, AMajordomo, Client


class GRWorker(IOWorker):
    pass


class GRService(Service):
    pass


class GRMajordomo(AMajordomo):
    pass


class SRClient(Client):
    pass


async def test():
    Settings.setup()
    gr_majordomo = GRMajordomo(backend_addr="tcp://127.0.0.1:5555")
    gr_majordomo.bind("tcp://127.0.0.1:777")
    gr_majordomo.run()
    gr = GRService(name="")
    gr_worker = gr.create_worker(
        "io", GRWorker, "tcp://127.0.0.1:5555"
    )
    print(gr_worker.service)
    if gr_worker.service is not gr:
        return
    print(gr_worker.interfaces)
    gr_worker.run()
    await asyncio.sleep(5)
    gr_cli = SRClient("tcp://127.0.0.1:777")
    i = 100
    print("start test")
    try:
        while i:
            result = gr_cli.SRkv.atest("a", "b", "c", time=time())
            print(await result)
            i -= 1
    except Exception:
        for line in format_exception(*sys.exc_info()):
            print(line)
        gr_cli.disconnect()
        await gr_worker.stop()
        gr_majordomo.stop()


if __name__ == "__main__":
    asyncio.run(test())
