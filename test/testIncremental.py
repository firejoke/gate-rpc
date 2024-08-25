# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/1/8 15:52
import asyncio
import os
import pickle
import random
import secrets
import selectors
import sys
import time
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from logging import exception
from pathlib import Path
from traceback import format_tb
from typing import Optional



base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.global_settings import Settings
from gaterpc.utils import (
    HugeData, UnixEPollEventLoopPolicy, _incremental_compress,
    _incremental_decompress, msg_pack, run_in_executor,
)
import testSettings


Settings.configure("USER_SETTINGS", testSettings)
Settings.setup()
tc_file = base_path.joinpath("test_compress")
com_module = "bz2"


def test_incremental_compress(hdata: HugeData):
    cd = b""
    os.set_inheritable(hdata._p[1], True)
    args = (
        hdata.compress_module, hdata.compress_level,
        hdata.data.name, hdata._p[1], hdata.end_tag
    )
    for arg in args:
        print(arg)
        pickle.dumps(arg)
    executor = ProcessPoolExecutor()
    func = partial(_incremental_compress, *args)
    fu = executor.submit(func)
    # _incremental_compress(*args)
    sel = selectors.DefaultSelector()
    sel.register(hdata._p[0], selectors.EVENT_READ)
    check_fu = True
    _continue = 1
    try:
        while _continue:
            if check_fu:
                if fu.done():
                    check_fu = False
                    executor.shutdown()
                    if exc := fu.exception():
                        raise exc
            events = sel.select(1)
            for key, mask in events:
                if mask == selectors.EVENT_READ:
                    d = os.read(hdata._p[0], hdata.blksize)
                    if d.endswith(hdata.end_tag):
                        cd += d[:-1 * len(hdata.end_tag)]
                        _continue = 0
                    else:
                        cd += d
        return cd
    finally:
        sel.unregister(hdata._p[0])
        with tc_file.open("wb") as f:
            f.write(cd)
        hdata.destroy()


def test_incremental_decompress():
    dd = b""
    with tc_file.open("rb") as f:
        ts = f.read()
    hdata = HugeData(
        Settings.HUGE_DATA_END_TAG,
        Settings.HUGE_DATA_EXCEPT_TAG,
        compress_module=com_module
    )
    os.set_inheritable(hdata.data[0], True)
    os.set_inheritable(hdata._p[1], True)
    args = (
        hdata.compress_module,
        hdata.data[0], hdata._p[1],
        hdata.end_tag
    )
    for arg in args:
        pickle.dumps(arg)
    executor = ProcessPoolExecutor()
    func = partial(
        _incremental_decompress, *args, max_length=1000
    )
    fu = executor.submit(func)
    ts_len = len(ts)
    print(f"ts len: {ts_len}")
    sel = selectors.DefaultSelector()
    sel.register(hdata._p[0], selectors.EVENT_READ)
    sel.register(hdata.data[1], selectors.EVENT_WRITE)
    index = 0
    _write = True
    _stream_end = False
    check_fu = True
    _continue = 1
    try:
        while _continue:
            if check_fu:
                if fu.done():
                    check_fu = False
                    executor.shutdown()
                    if exc := fu.exception():
                        raise exc
            events = sel.select(1)
            for key, mask in events:
                if mask == selectors.EVENT_WRITE and _write:
                    wdata = ts[index: (index := index+1000)]
                    if wdata:
                        os.write(hdata.data[1], wdata)
                    else:
                        os.write(hdata.data[1], hdata.end_tag)
                        _write = False
                if mask == selectors.EVENT_READ:
                    d = os.read(hdata._p[0], hdata.blksize)
                    if d.endswith(hdata.end_tag):
                        d = d[:-1 * len(hdata.end_tag)]
                        dd += d
                        _continue = 0
                    else:
                        dd += d
        return dd
    finally:
        sel.unregister(hdata._p[0])
        sel.unregister(hdata.data[1])
        print("hd.destroy")
        hdata.destroy()


async def compress_agenerator(hdata:HugeData):
    sel = selectors.DefaultSelector()
    loop = asyncio.get_running_loop()
    args = (
        hdata.compress_module, hdata.compress_level,
        hdata.data.name, hdata._p[1], hdata.end_tag
    )
    func = partial(_incremental_compress, *args)
    _wait: Optional[asyncio.Future] = None
    _wait_id = ""

    def _read():
        nonlocal _wait, _wait_id
        _wait = loop.create_future()
        _wait_id = id(_wait)
        _wait.set_result(os.read(hdata._p[0], hdata.blksize))

    sel.register(hdata._p[0], selectors.EVENT_READ)
    read_key = (sel.get_key(hdata._p[0]), selectors.EVENT_READ)
    executor = ProcessPoolExecutor()
    try:
        f = loop.run_in_executor(executor, func)
        check_fu = True
        cont = 1
        while cont:
            if check_fu:
                if f.done():
                    print("future run end")
                    executor.shutdown()
                    check_fu = False
                    if exc := f.exception():
                        raise exc
            # if _wait:
            #     if _wait_id != id(_wait):
            #         raise RuntimeError("id error")
            #     try:
            #         data = await asyncio.wait_for(_wait, hdata.get_timeout)
            #         if data.endswith(hdata.end_tag):
            #             yield data[:-1 * len(hdata.end_tag)]
            #             break
            #         else:
            #             yield data
            #     except asyncio.TimeoutError:
            #         pass
            #     _wait = None
            # await asyncio.sleep(0)
            events = sel.select(1)
            if read_key in events:
                data = os.read(hdata._p[0], hdata.blksize)
                if data.endswith(hdata.end_tag):
                    yield data[:-1 * len(hdata.end_tag)]
                    cont = 0
                else:
                    yield data
    # except Exception as error:
    #     except_info = (
    #             str(error.__class__),
    #             str(error),
    #             "".join(format_tb(error.__traceback__))
    #         )
    #     print(except_info)
    #     await asyncio.sleep(1)
    finally:
        # loop.remove_reader(hdata._p[0])
        sel.unregister(hdata._p[0])


def send_rawd(hdata: HugeData, raw_d):
    hdata.add_data(raw_d)
    hdata.flush()
    print(f"send end.")


async def incremental_compress(hdata: HugeData, raw_d=b""):
    loop = asyncio.get_running_loop()
    cd = b""
    texecutor = ThreadPoolExecutor()
    send_t = None
    if raw_d:
        send_t = loop.run_in_executor(texecutor, send_rawd, hdata, raw_d)
    try:
        async for dd in hdata.compress(loop):
            cd += dd
        if send_t:
            await send_t
        with tc_file.open("wb") as f:
            f.write(cd)
        cdl = len(cd)
        return cdl
    finally:
        texecutor.shutdown(False, cancel_futures=True)


async def decompress_agenerator(hdata: HugeData):
    sel = selectors.DefaultSelector()
    loop = asyncio.get_running_loop()
    with tc_file.open("rb") as f:
        ts = f.read()
    args = (
        hdata.compress_module,
        hdata.data[0], hdata._p[1],
        hdata.end_tag
    )
    func = partial(_incremental_decompress, *args, max_length=1000)
    r_wait: Optional[asyncio.Future] = None
    r_wait_id = ""
    w_wait: Optional[asyncio.Future] = None
    w_wait_id = ""

    def _write():
        nonlocal w_wait, w_wait_id
        w_wait = loop.create_future()
        w_wait_id = id(w_wait)
        w_wait.set_result(True)

    def _read():
        nonlocal r_wait, r_wait_id
        r_wait = loop.create_future()
        r_wait_id = id(r_wait)
        r_wait.set_result(os.read(hdata._p[0], hdata.blksize))

    sel.register(hdata._p[0], selectors.EVENT_READ)
    read_key = (sel.get_key(hdata._p[0]), selectors.EVENT_READ)
    sel.register(hdata.data[1], selectors.EVENT_WRITE)
    write_key = (sel.get_key(hdata.data[1]), selectors.EVENT_WRITE)
    executor = ProcessPoolExecutor()
    index = 0
    ts_len = len(ts)
    _write = True
    _stream_end = False
    print(f"ts len: {ts_len}")
    try:
        f = loop.run_in_executor(executor, func)
        check_fu = True
        cont = 1
        while cont:
            data = b""
            if check_fu:
                if f.done():
                    check_fu = False
                    executor.shutdown()
                    if exc := f.exception():
                        raise exc
            events = sel.select(1)
            # print(events)
            if read_key in events:
                data = os.read(hdata._p[0], hdata.blksize)
                if data.endswith(hdata.end_tag):
                    data = data[:-1 * len(hdata.end_tag)]
                    cont = 0
            if write_key in events and _write:
                wdata = ts[index: (index := index+1000)]
                if wdata:
                    os.write(hdata.data[1], wdata)
                else:
                    os.write(hdata.data[1], hdata.end_tag)
                    _write = False
            if data:
                yield data
    finally:
        sel.unregister(hdata.data[1])
        sel.unregister(hdata._p[0])


def send_dd(hdata: HugeData, dd):
        # events = sel.select(1)
        # if write_key in events:
    hdata.add_data(dd)
    hdata.flush()
    print("send end.")


async def incremental_decompress(hdata: HugeData, dd=b""):
    loop = asyncio.get_running_loop()
    raw_d = b""
    # executor = ProcessPoolExecutor()
    texecutor = ThreadPoolExecutor()
    # t = loop.create_task(send_dd(hd))
    send_t = None
    if dd:
        send_t = loop.run_in_executor(texecutor, send_dd, hdata, dd)
    st = loop.time()
    try:
        async for _d in hdata.decompress(loop, max_length=1024):
            raw_d += _d
        if send_t:
            await send_t
    # await asyncio.gather(dd, tt)
    # async for data in hd.decompress(1000):
    #     if throw:
    #         raise RuntimeError(data)
    #     if data == hd.except_tag:
    #         throw = True
    #     else:
    #         dd += data
        return raw_d
    finally:
        # executor.shutdown(False, cancel_futures=True)
        texecutor.shutdown(False, cancel_futures=True)
        print(f"raw_d len: {len(raw_d)}")
    # except Exception as error:
    #     except_info = (
    #         str(error.__class__),
    #         str(error),
    #         "".join(format_tb(error.__traceback__))
    #     )
    #     print(except_info)


def main(mode="all"):
    # asyncio.set_event_loop_policy(UnixEPollEventLoopPolicy())
    bs = ""
    if mode in ("all", "compress"):
        i = random.randint(10000, 30000)
        while i:
            bs += secrets.token_hex()
            i -= 1
        print(f"raw bs len: {len(bs)}")
        bs = msg_pack(bs)
        bsl = len(bs)
        print(f"bs len: {bsl}")
        print(asyncio.get_event_loop_policy())
        hd = HugeData(
            Settings.HUGE_DATA_END_TAG,
            Settings.HUGE_DATA_EXCEPT_TAG,
            # data=bs,
            compress_module=com_module,
            compress_level=Settings.HUGE_DATA_COMPRESS_LEVEL
        )
        print("asyncio.run(incremental_compress(hd)")
        cdl = asyncio.run(incremental_compress(hd, bs), debug=True)
        hd.destroy()
        print(f"cd len: {cdl}")
        print(cdl / bsl)

    if mode in ("all", "decompress"):
        with tc_file.open("rb") as f:
            ts = f.read()
        hd = HugeData(
            Settings.HUGE_DATA_END_TAG,
            Settings.HUGE_DATA_EXCEPT_TAG,
            data=ts,
            compress_module=com_module,
        )
        print(hd.data)
        print("asyncio.run(incremental_decompress(hd)")
        dd = asyncio.run(incremental_decompress(hd,), debug=True)
        hd.destroy()
        print(f"dd len: {len(dd)}")
        if bs:
            is_ = dd == bs
            print(f"dd == bs: {is_}")
            if not is_:
                raise RuntimeError

    print("main end")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
    # test_incremental_compress(hd)
    # test_incremental_decompress()
    # snapshot = tracemalloc.take_snapshot()
    # top_stats = snapshot.statistics("lineno")
    # for stat in top_stats:
    #     print(stat)
