# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/22 16:36
import asyncio
import atexit
import bz2
import copy
import functools
import inspect
import io
import lzma
import os
import platform
import queue
import selectors
import struct
import sys
import threading
import time
import warnings

import zlib

from logging import Handler, getLogger
from multiprocessing import Manager
from multiprocessing.managers import (
    ArrayProxy, DictProxy, ListProxy,
    ValueProxy,
)
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.resource_tracker import unregister
from importlib import import_module
from pathlib import Path
from functools import (
    WRAPPER_ASSIGNMENTS, _make_key, partial, update_wrapper,
    wraps, lru_cache,
)

from collections import deque, namedtuple
from collections.abc import AsyncGenerator, Callable, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Optional, TypeVar, Union, ByteString, overload

import msgpack

from .exceptions import BadGzip, HugeDataException, RemoteException


logger = getLogger("commands")
SyncManager = Manager()


class UnixEPollEventLoopPolicy(asyncio.unix_events.DefaultEventLoopPolicy):
    _loop_factory = asyncio.unix_events.SelectorEventLoop

    def new_event_loop(self):
        return self._loop_factory(selectors.EpollSelector())


def singleton(cls):
    instances = dict()

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances.get(cls)

    return get_instance


def resolve_module_attr(s: str):
    name = s.split('.')
    module_name = name.pop(0)
    try:
        found = import_module(module_name)
        for frag in name:
            try:
                found = getattr(found, frag)
            except AttributeError:
                import_module(f"{module_name}.{frag}")
                found = getattr(found, frag)
        return found
    except ImportError as e:
        v = ValueError(f"Cannot resolve {s}: {e}")
        raise v from e


def check_socket_addr(socket_addr: Optional[str]) -> Optional[str]:
    if socket_addr is None:
        return socket_addr
    system = platform.system().lower()
    proto = socket_addr.split("://")[0]
    if proto == "ipc" and system == "windows":
        raise SystemError("ipc protocol does not work on Windows.")
    if proto not in ("inproc", "ipc", "tcp", "pgm", "epgm"):
        raise SystemError(f"The protocol \"{proto}\" is not supported.")
    return socket_addr


def to_bytes(s: Union[str, bytes, float, int]):
    if isinstance(s, bytes):
        return s
    if isinstance(s, str):
        return s.encode("utf-8")
    fmt = "!d"
    if isinstance(s, int):
        if -128 <= s <= 127:
            fmt = "!b"
        elif -32768 <= s <= 32767:
            fmt = "!h"
        elif -2147483648 <= s <= 2147483647:
            fmt = "!i"
    return struct.pack(fmt, s)


def from_bytes(b: bytes) -> Union[str, float, int]:
    fmt = {
        1: "!b",
        2: "!h",
        4: "!i",
        8: "!d"
    }
    if b_len := len(b) in fmt:
        try:
            return struct.unpack(fmt[b_len], b)[0]
        except struct.error:
            pass
    return b.decode("utf-8")


def encrypt(data):
    # 在进程池执行器中执行
    hash_str = data
    return hash_str


def decrypt(hash_str):
    # 在进程池执行器中执行
    data = hash_str
    return data


async def generator_to_agenerator(generator: Generator) -> AsyncGenerator:
    for element in generator:
        yield element


async def throw_exception_agenerator(ag: AsyncGenerator, exc: BaseException):
    try:
        await ag.athrow(exc)
    except StopAsyncIteration:
        pass


class Empty:

    def __bool__(self):
        return False


empty = Empty()

_CacheInfo = namedtuple("CacheInfo", ["hits", "misses", "maxsize", "currsize"])


class LRUCache:
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self.hits = self.misses = 0
        self.full = False
        self.root = []
        self.root[:] = [self.root, self.root, None, None]
        self.cache = dict()

    def __contains__(self, item):
        if item in self.cache:
            return True
        return False

    def __setitem__(self, key, value):
        if key in self.cache:
            return
        if self.full:
            oldroot = self.root
            oldroot[self.KEY] = key
            oldroot[self.RESULT] = value
            self.root = oldroot[self.NEXT]
            oldkey = self.root[self.KEY]
            oldresult = self.root[self.RESULT]
            self.root[self.KEY] = self.root[self.RESULT] = None
            del self.cache[oldkey]
            self.cache[key] = oldroot
        else:
            last = self.root[self.PREV]
            link = [last, self.root, key, value]
            last[self.NEXT] = self.root[self.PREV] = self.cache[key] = link
            self.full = len(self.cache) >= self.maxsize

    def __getitem__(self, item):
        if (link := self.cache.get(item, None)) is not None:
            link_prev, link_next, _key, result = link
            link_prev[self.NEXT] = link_next
            link_next[self.PREV] = link_prev
            last = self.root[self.PREV]
            last[self.NEXT] = self.root[self.PREV] = link
            link[self.PREV] = last
            link[self.NEXT] = self.root
            self.hits += 1
            return result
        self.misses += 1
        raise KeyError

    def __repr__(self):
        return (f"hits: {self.hits}, misses: {self.misses},"
                f" maxsize: {self.maxsize}, cache size: {len(self.cache)}")


def _coroutine_lru_cache_wrapper(user_function, maxsize, typed, _CacheInfo):
    loop = asyncio.get_event_loop()
    sentinel = object()
    make_key = _make_key
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3

    cache = {}
    hits = misses = 0
    full = False
    cache_get = cache.get
    cache_len = cache.__len__
    lock = threading.RLock()
    root = []
    root[:] = [root, root, None, None]

    if maxsize == 0:

        async def wrapper(*args, **kwds):
            nonlocal misses
            misses += 1
            result = await user_function(*args, **kwds)
            return result

    elif maxsize is None:

        async def wrapper(*args, **kwds):
            nonlocal hits, misses
            key = make_key(args, kwds, typed)
            result = cache_get(key, sentinel)
            if result is not sentinel:
                hits += 1
                return await result
            cache[key] = f = loop.create_future()
            misses += 1
            result = await user_function(*args, **kwds)
            f.set_result(result)
            return result

    else:

        async def wrapper(*args, **kwds):
            nonlocal root, hits, misses, full
            key = make_key(args, kwds, typed)
            with lock:
                link = cache_get(key)
                if link is not None:
                    link_prev, link_next, _key, result = link
                    link_prev[NEXT] = link_next
                    link_next[PREV] = link_prev
                    last = root[PREV]
                    last[NEXT] = root[PREV] = link
                    link[PREV] = last
                    link[NEXT] = root
                    hits += 1
                    return result
                misses += 1
            f = loop.create_future()
            with lock:
                if key in cache:
                    pass
                elif full:
                    oldroot = root
                    oldroot[KEY] = key
                    oldroot[RESULT] = f
                    root = oldroot[NEXT]
                    oldkey = root[KEY]
                    oldresult = root[RESULT]
                    root[KEY] = root[RESULT] = None
                    del cache[oldkey]
                    cache[key] = oldroot
                else:
                    last = root[PREV]
                    link = [last, root, key, f]
                    last[NEXT] = root[PREV] = cache[key] = link
                    full = (cache_len() >= maxsize)

            f.set_result(await user_function(*args, **kwds))
            return await f

    def cache_info():
        """Report cache statistics"""
        with lock:
            return _CacheInfo(hits, misses, maxsize, cache_len())

    def cache_clear():
        """Clear the cache and cache statistics"""
        nonlocal hits, misses, full
        with lock:
            cache.clear()
            root[:] = [root, root, None, None]
            hits = misses = 0
            full = False

    wrapper.cache_info = cache_info
    wrapper.cache_clear = cache_clear
    return wrapper


def coroutine_lru_cache(maxsize=128, typed=False):
    if isinstance(maxsize, int):
        # Negative maxsize is treated as 0
        if maxsize < 0:
            maxsize = 0
    elif asyncio.iscoroutinefunction(maxsize) and isinstance(typed, bool):
        # The user_function was passed in directly via the maxsize argument
        user_function, maxsize = maxsize, 128
        wrapper = _coroutine_lru_cache_wrapper(
            user_function, maxsize, typed, _CacheInfo
        )
        wrapper.cache_parameters = lambda: {'maxsize': maxsize, 'typed': typed}
        return update_wrapper(wrapper, user_function)
    elif maxsize is not None:
        raise TypeError(
            'Expected first argument to be an integer, a callable, or None'
        )

    def decorating_function(user_function):
        wrapper = _coroutine_lru_cache_wrapper(
            user_function, maxsize, typed, _CacheInfo
        )
        wrapper.cache_parameters = lambda: {'maxsize': maxsize, 'typed': typed}
        return update_wrapper(wrapper, user_function)

    return decorating_function


class Validator:

    def __init__(self, error_message=None):
        if error_message:
            self.error_message = error_message

    def __call__(self, value):
        return value


class TypeValidator(Validator):
    error_message = "require {require_type}."

    def __init__(self, require_type, error_message=None):
        self.require_type = require_type
        super().__init__(error_message)

    def __call__(self, value):
        if not isinstance(value, self.require_type):
            raise TypeError(
                self.error_message.format(require_type=self.require_type)
            )
        return value


def ensure_mkdir(p: Union[str, Path]):
    if isinstance(p, str):
        p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


_I = TypeVar("_I")
_PV = TypeVar("_PV")


class LazyAttribute:

    def __init__(
        self, raw: Any = empty,
        render: Callable[[_I, _PV], Any] = None,
        process: Callable[[_I, _PV], Any] = None
    ):
        self._raw = raw
        self._process = process
        self._render = render

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, instance, owner=None):
        if self._render:
            return self._render(instance, self._raw)
        return self._raw

    def __set__(self, instance, value):
        if self._process:
            self._raw = self._process(instance, value)
        else:
            self._raw = value


class QueueListener(object):
    _sentinel = None

    def __init__(
        self, handlers: Union[list[Handler], tuple[Handler]] = None,
        respect_handler_level=True
    ):
        self.respect_handler_level = respect_handler_level
        self.queue = queue.Queue()
        self.handlers = dict()
        self._thread = None
        if handlers:
            self.add_handlers(handlers)
        atexit.register(self.stop)
        self.start()

    def add_handlers(self, handlers: Union[list[Handler], tuple[Handler]]):
        for handler in handlers:
            self.handlers[handler.get_name()] = handler

    def remove_handler(self, handler: Handler):
        self.handlers.pop(handler.get_name())

    def dequeue(self):
        return self.queue.get()

    def prepare(self, record):
        return record

    def handle(self, record):
        record = self.prepare(record)
        handler = self.handlers.get(record.h_name)
        if handler:
            if not self.respect_handler_level:
                process = True
            else:
                process = record.levelno >= handler.level
            if process:
                handler.handle(record)

    def enqueue_sentinel(self):
        self.queue.put_nowait(self._sentinel)

    def _monitor(self):
        q = self.queue
        has_task_done = hasattr(q, 'task_done')
        while True:
            try:
                record = self.dequeue()
                if record is self._sentinel:
                    if has_task_done:
                        q.task_done()
                    break
                self.handle(record)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break
            except Exception as e:
                warnings.warn(e.__repr__())

    def start(self):
        self._thread = t = threading.Thread(target=self._monitor)
        t.daemon = True
        t.start()

    def stop(self):
        if self._thread:
            self.enqueue_sentinel()
            self._thread.join()
            self._thread = None


class AQueueListener(object):
    _sentinel = None

    def __init__(
        self, handlers: Union[list[Handler], tuple[Handler]] = None,
        respect_handler_level=True
    ):
        self.respect_handler_level = respect_handler_level
        self.queue = asyncio.Queue()
        self.handlers = dict()
        self._task: Optional[asyncio.Task] = None
        if handlers:
            self.add_handlers(handlers)
        atexit.register(self.stop)
        # self._loop.call_soon(self.start)
        self.start()

    def add_handlers(self, handlers: Union[list[Handler], tuple[Handler]]):
        for handler in handlers:
            self.handlers[handler.get_name()] = handler

    def remove_handler(self, handler: Handler):
        self.handlers.pop(handler.get_name())

    async def dequeue(self):
        return await self.queue.get()

    def prepare(self, record):
        return record

    async def handle(self, record):
        record = self.prepare(record)
        handler = self.handlers.get(record.h_name)
        if handler:
            if not self.respect_handler_level:
                process = True
            else:
                process = record.levelno >= handler.level
            if process:
                if asyncio.iscoroutinefunction(handler.handle):
                    await handler.handle(record)
                else:
                    handler.handle(record)

    def enqueue_sentinel(self):
        self.queue.put_nowait(self._sentinel)

    async def _monitor(self):
        q = self.queue
        has_task_done = hasattr(q, 'task_done')
        while True:
            try:
                record = await self.dequeue()
                if record is self._sentinel:
                    if has_task_done:
                        q.task_done()
                    break
                await self.handle(record)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break
            except Exception as e:
                warnings.warn(e.__repr__())

    def start(self):
        self._task = asyncio.create_task(self._monitor())

    def stop(self):
        if self._task:
            self.enqueue_sentinel()
            # self._task.cancel()


class AQueueHandler(Handler):

    def __init__(
        self, *args,
        handler_class: Union[Handler, str] = None,
        listener: str = "gaterpc.utils.singleton_queue_listener",
        **kwargs
    ):
        Handler.__init__(self)
        if isinstance(handler_class, str):
            handler_class = resolve_module_attr(handler_class)
        self.handler = handler_class(*args, **kwargs)
        self._name = f"QueueHandler{id(self.handler)}"
        self.listener = resolve_module_attr(listener)()
        self.h_name = f"{self._name}-handler"
        self.handler.set_name(self.h_name)
        self.listener.add_handlers((self.handler,))

    def prepare(self, record):
        msg = self.format(record)
        record = copy.copy(record)
        record.message = msg
        record.msg = msg
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        record.h_name = self.h_name
        return record

    def enqueue(self, record):
        self.listener.queue.put_nowait(record)

    def emit(self, record):
        try:
            self.enqueue(self.prepare(record))
        except Exception as e:
            self.handleError(record)
            warnings.warn(e.__repr__())

    def close(self):
        if self.listener:
            self.listener.remove_handler(self.handler)
        super().close()


singleton_aqueue_listener = singleton(AQueueListener)
singleton_queue_listener = singleton(QueueListener)


class MessagePack(object):
    """
    用于配置全局的msgpack
    """

    def __init__(self):
        self.prepare_pack: Optional[Callable] = None
        self.unpack_object_hook: Optional[Callable] = None
        self.unpack_object_pairs_hook: Optional[Callable] = None
        self.unpack_list_hook: Optional[Callable] = None

    def __setattr__(self, key, value):
        if key in (
                "prepare_pack",
                "unpack_object_hook",
                "unpack_object_pairs_hook",
                "unpack_list_hook"
        ) and value and not isinstance(value, Callable):
            raise TypeError(f"`{key}` is not callable")
        super().__setattr__(key, value)

    def __delattr__(self, item):
        if item in (
                "prepare_pack",
                "unpack_object_hook",
                "unpack_object_pairs_hook",
                "unpack_list_hook"
        ):
            setattr(self, item, None)
        super().__delattr__(item)

    def loads(self, data: bytes) -> Any:
        obj = msgpack.unpackb(
            data,
            object_hook=self.unpack_object_hook,
            object_pairs_hook=self.unpack_object_pairs_hook,
            list_hook=self.unpack_list_hook
        )
        return obj

    def dumps(self, obj) -> ByteString:
        data = msgpack.packb(obj, default=self.prepare_pack)
        return data


class MsgPackError(ValueError):
    pass


class MsgUnpackError(ValueError):
    pass


message_pack = MessagePack()


def msg_pack(obj: Any) -> ByteString:
    if isinstance(obj, ValueProxy):
        obj = obj.get()
    elif isinstance(obj, ArrayProxy):
        obj = obj.tolist()
    elif isinstance(obj, (DictProxy, ListProxy)):
        obj = obj._getvalue()
    try:
        data = message_pack.dumps(obj)
    except Exception as e:
        raise MsgPackError(f"{obj} pack failed.").with_traceback(
            e.__traceback__
        )
    return data


def msg_unpack(data: ByteString) -> Any:
    try:
        obj = message_pack.loads(data)
    except Exception as e:
        raise MsgUnpackError(f"{data} unpack failed.").with_traceback(
            e.__traceback__
        )
    return obj


def ensure_shm_unlink(shm: SharedMemory):
    try:
        if shm.buf is not None:
            shm.buf.release()
        shm.close()
        shm.unlink()
    except FileNotFoundError:
        try:
            unregister(shm._name, "shared_memory")
        except KeyError:
            pass
    except KeyError:
        pass
    except ImportError:
        pass


class StreamReply(AsyncGenerator):

    def __init__(self, end_message, maxsize=0, timeout=0):
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        self.end_message = end_message
        self.replies = asyncio.Queue(maxsize, loop=self._loop)
        self.timeout = timeout
        self._exit = False
        self._pause = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._exit:
            raise GeneratorExit
        if self.timeout:
            value = await asyncio.wait_for(
                self.replies.get(), self.timeout, loop=self._loop
            )
        else:
            value = self.replies.get_nowait()
        self.replies.task_done()
        if value == self.end_message:
            await asyncio.sleep(0.1)
            self._exit = True
            raise StopAsyncIteration
        if self._pause and isinstance(value, BaseException):
            self._pause = False
            raise value
        return value

    async def asend(self, value):
        if value is not None:
            await self.replies.put(value)
        return

    async def athrow(self, __type, value=None, traceback=None):
        self._pause = True
        if traceback:
            await self.replies.put(__type.with_traceback(traceback))
        elif isinstance(value, BaseException):
            await self.replies.put(value)
        else:
            if value:
                __type = __type(value)
            await self.replies.put(__type)

    async def aclose(self):
        if not self._exit:
            self._exit = True


class GzipCompressor(object):

    def __init__(self, compresslevel):
        self.compresslevel = compresslevel
        self.crc = zlib.crc32(b"")
        self.size = 0
        self._add_header = True
        self._compressor = zlib.compressobj(
            self.compresslevel,
            zlib.DEFLATED,
            -zlib.MAX_WBITS,
            zlib.DEF_MEM_LEVEL,
            zlib.Z_DEFAULT_STRATEGY
        )

    def _generate_header(self, mtime=None) -> bytes:
        header = b"\037\213\010\000"
        if not mtime:
            mtime = time.time()
        header += struct.pack("<L", int(mtime))
        if self.compresslevel == 9:
            header += b"\002"
        elif self.compresslevel == 1:
            header += b"\004"
        else:
            header += b"\000"
        header += b"\377"
        return header

    def compress(self, data):
        if isinstance(data, bytes):
            length = len(data)
        else:
            data = memoryview(data)
            length = data.nbytes
        if length > 0:
            self.crc = zlib.crc32(data, self.crc)
            data = self._compressor.compress(data)
            self.size += length
            if self._add_header:
                data = self._generate_header() + data
                self._add_header = False
        return data

    def flush(self):
        data = self._compressor.flush()
        data += struct.pack("<L", self.crc)
        data += struct.pack("<L", self.size & 0xffffffff)
        return data


class GzipDecompressor(object):

    def __init__(self):
        self._buffer = b""
        self._crc = zlib.crc32(b"")
        self._stream_size = 0
        self._new_member = True
        self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)

    def _remove_header(self, data):
        index = 0
        magic = data[index: (index := index + 2)]
        if magic != b"\037\213":
            raise BadGzip("Not a gzipped data.")
        method, flag, self.last_mtime = struct.unpack(
            "<BBIxx", data[index: (index := index + 8)]
        )
        if method != 8:
            raise BadGzip("Unknown conmpression method")
        if flag & 4:
            extra_len, = struct.unpack(
                "<H", data[index: (index := index + 2)]
            )
            index += extra_len
        if flag & 8:
            while True:
                s = data[(index := index + 1)]
                if not s or s == b'\000':
                    break
        if flag & 16:
            while True:
                s = data[(index := index + 1)]
                if not s or s == b'\000':
                    break
        if flag & 2:
            index += 2
        return data[index:]

    @property
    def eof(self):
        return self._decompressor.eof

    @property
    def needs_input(self):
        return self._decompressor.unconsumed_tail == b""

    def _read_eof(self):
        if buffer := self._buffer[:8]:
            crc32, isize = struct.unpack("<II", buffer)
            if crc32 != self._crc:
                raise BadGzip(f"CRC check failed {hex(crc32)} != {hex(self._crc)}")
            elif isize != (self._stream_size & 0xffffffff):
                raise BadGzip("Incorrect length of data produced")

    def decompress(self, data, max_length=0):
        if max_length < 0:
            max_length = 0
        if self._new_member:
            data = self._remove_header(data)
            self._new_member = False
        data += self._buffer
        data = self._decompressor.decompress(data, max_length)
        self._buffer = self._decompressor.unconsumed_tail
        if data:
            self._crc = zlib.crc32(data, self._crc)
        self._stream_size += len(data)
        return data


def _incremental_compress(
    compress_module: str, compress_level: int,
    in_data: Union[str, int], out_data: int, end_tag: bytes,
    blksize: int = 1000
):
    """
    :param compress_module: 压缩模块
    :param compress_level: 压缩级别
    :param in_data: 输入数据，管道读取端的文件描述符，或者 SharedMemory 的名
    :param out_data: 输出数据，管道写入端的文件描述符
    :param end_tag: 结束标记
    :param blksize: 每一块数据的大小
    """
    sel = selectors.DefaultSelector()
    sel.register(out_data, selectors.EVENT_WRITE)
    write_key = (sel.get_key(out_data), selectors.EVENT_WRITE)
    if isinstance(in_data, str):
        shm = SharedMemory(name=in_data)
        shm_index = 0

        def read_data(events):
            nonlocal shm_index
            data = shm.buf[
                   shm_index:
                   (shm_index := shm_index + io.DEFAULT_BUFFER_SIZE)
                   ].tobytes()
            if data:
                return data
            else:
                return end_tag

        def clean():
            try:
                shm.close()
            except BufferError as e:
                # logger.warning(f"_incremental_compress.clean: {e}")
                pass

    elif isinstance(in_data, int):
        sel.register(in_data, selectors.EVENT_READ)
        read_key = (sel.get_key(in_data), selectors.EVENT_READ)

        def read_data(events):
            if read_key in events:
                dl = os.read(in_data, 2)
                d = os.read(in_data, int.from_bytes(dl, sys.byteorder))
                return d
            return None

        def clean():
            sel.unregister(in_data)

    else:
        raise TypeError("in_data")

    try:
        if compress_module == "gzip":
            compressor = GzipCompressor(compress_level)
        elif compress_module == "bz2":
            compressor = bz2.BZ2Compressor(compress_level)
        elif compress_module == "lzma":
            compressor = lzma.LZMACompressor(preset=compress_level)
        else:
            compress_module = resolve_module_attr(compress_module)
            compressor = compress_module.compressor(compress_level)
        buffer = b""
        stream_end = False
        finish = False
        while 1:
            events = sel.select(1)
            if stream_end and not finish:
                buffer += compressor.flush()
                finish = True
            elif (len(buffer) > blksize) or (stream_end and buffer):
                if write_key in events:
                    c_d, buffer = (
                        buffer[:blksize],
                        buffer[blksize:]
                    )
                    c_d = len(c_d).to_bytes(2, sys.byteorder) + c_d
                    os.write(out_data, c_d)
            elif not stream_end:
                raw_d = read_data(events)
                if raw_d == end_tag:
                    stream_end = True
                elif raw_d:
                    buffer += compressor.compress(raw_d)
            else:
                break
    except Exception as error:
        raise HugeDataException(error)
    finally:
        os.write(out_data, len(end_tag).to_bytes(2, sys.byteorder) + end_tag)
        clean()
        sel.unregister(out_data)
        sel.close()


def _incremental_decompress(
    compress_module: str,
    in_data: Union[str, int], out_data: int,
    end_tag: bytes,
    max_length=-1, blksize=1000
):
    """
    :param compress_module: 压缩模块
    :param in_data: 输入数据，管道读取端的文件描述符，或者 SharedMemory 的名
    :param out_data: 输出数据，管道写入端的文件描述符
    :param end_tag: 结束标记
    :param max_length: 解压缩参数
    :param blksize: 每一块数据的大小
    """
    sel = selectors.DefaultSelector()
    sel.register(out_data, selectors.EVENT_WRITE)
    write_key = (sel.get_key(out_data), selectors.EVENT_WRITE)
    if isinstance(in_data, str):
        shm = SharedMemory(name=in_data)
        shm_index = 0

        def read_data(events):
            nonlocal shm_index
            data = shm.buf[
                   shm_index:
                   (shm_index := shm_index + io.DEFAULT_BUFFER_SIZE)
                   ].tobytes()
            if data:
                return data
            else:
                return end_tag

        def clean():
            try:
                shm.close()
            except BufferError as e:
                # logger.warning(f"_incremental_decompress.clean: {e}")
                pass

    elif isinstance(in_data, int):
        sel.register(in_data, selectors.EVENT_READ)
        read_key = (sel.get_key(in_data), selectors.EVENT_READ)

        def read_data(events):
            if read_key in events:
                dl = os.read(in_data, 2)
                return os.read(in_data, int.from_bytes(dl, sys.byteorder))
            return None

        def clean():
            sel.unregister(in_data)

    else:
        raise TypeError("in_data")

    try:
        if compress_module == "gzip":
            decompressor = GzipDecompressor()
        elif compress_module == "bz2":
            decompressor = bz2.BZ2Decompressor()
        elif compress_module == "lzma":
            decompressor = lzma.LZMADecompressor()
        else:
            compress_module = resolve_module_attr(compress_module)
            decompressor = compress_module.decompressor()
        buffer = b""
        stream_end = False
        while 1:
            events = sel.select(1)
            if write_key in events:
                if buffer:
                    d_d, buffer = (
                        buffer[:blksize],
                        buffer[blksize:]
                    )
                    d_d = len(d_d).to_bytes(2, sys.byteorder) + d_d
                    os.write(out_data, d_d)
            if not buffer:
                if decompressor.eof:
                    break
                if decompressor.needs_input:
                    if stream_end:
                        raise OSError("The compressed data is incomplete")
                    c_d = read_data(events)
                else:
                    c_d = b""
                if c_d and c_d.endswith(end_tag):
                    c_d = c_d[:-1 * len(end_tag)]
                    stream_end = True
                if c_d is not None:
                    buffer += decompressor.decompress(c_d, max_length)
    except Exception as error:
        raise HugeDataException(error)
    finally:
        os.write(out_data, len(end_tag).to_bytes(2, sys.byteorder) + end_tag)
        clean()
        sel.unregister(out_data)
        sel.close()


class HugeData:

    def __init__(
        self,
        end_tag,
        except_tag,
        *,
        data: Optional[bytes] = None,
        compress_module: str = "gzip",
        compress_level: int = 1,
        blksize: int = 1000
    ):
        self.sel = selectors.DefaultSelector()
        self.end_tag = end_tag
        self.except_tag = except_tag
        self.data: Union[SharedMemory, queue.Queue]
        if data:
            self.data = SharedMemory(create=True, size=len(data))
            self.data.buf[:len(data)] = data
        else:
            self.data: tuple = os.pipe()
        self.compress_module: str = compress_module
        self.compress_level: int = compress_level
        self.blksize: int = blksize
        self._p: tuple = os.pipe()
        self._write_buffer = b""
        self._write_end = False
        self._remote_exception = None

        atexit.register(self.destroy)

    def destroy(self):
        if isinstance(self.data, SharedMemory):
            ensure_shm_unlink(self.data)
        elif isinstance(self.data, tuple):
            os.close(self.data[0])
            os.close(self.data[1])
            self.data = None
        if self._p:
            os.close(self._p[0])
            os.close(self._p[1])
            self._p = None
        if self.sel:
            self.sel.close()
            self.sel = None
        atexit.unregister(self.destroy)

    def __del__(self):
        self.destroy()

    def add_data(self, data: bytes):
        if not isinstance(self.data, tuple):
            raise TypeError("data")
        if self._write_end:
            raise RuntimeError("pipe close")
        self._write_buffer += data

    def flush(self):
        self._write_end = True

    def _write_data1(self):
        if self._write_buffer:
            r_d, self._write_buffer = (
                self._write_buffer[:self.blksize],
                self._write_buffer[self.blksize:]
            )
            r_d = len(r_d).to_bytes(2, sys.byteorder) + r_d
            os.write(self.data[1], r_d)
        elif self._write_end:
            os.write(
                self.data[1],
                len(self.end_tag).to_bytes(2, sys.byteorder) + self.end_tag
            )

    async def compress(
        self, loop: asyncio.AbstractEventLoop
    ) -> AsyncGenerator[Union[bytes, tuple]]:
        clean_writer = False
        if isinstance(self.data, SharedMemory):
            args = (
                self.compress_module, self.compress_level,
                self.data.name, self._p[1], self.end_tag
            )
        else:
            args = (
                self.compress_module, self.compress_level,
                self.data[0], self._p[1], self.end_tag
            )

            loop.add_writer(self.data[1], self._write_data1)
            clean_writer = True

        func = partial(
            _incremental_compress, *args,
            blksize=self.blksize
        )
        read_q = asyncio.Queue()

        def _read():
            dl = os.read(self._p[0], 2)
            d = os.read(self._p[0], int.from_bytes(dl, sys.byteorder))
            read_q.put_nowait(d)

        loop.add_reader(self._p[0], _read)
        process_executor = ProcessPoolExecutor()
        f = loop.run_in_executor(process_executor, func)
        try:
            await asyncio.sleep(0)
            cont = 1
            read_num = 0
            while cont:
                await asyncio.sleep(0)
                if self._remote_exception:
                    remote_exception = msg_unpack(self._remote_exception)
                    raise RemoteException(remote_exception)
                try:
                    data = await asyncio.wait_for(read_q.get(), 1)
                    read_q.task_done()
                except asyncio.TimeoutError:
                    data = b""
                if data:
                    read_num += 1
                    if data.endswith(self.end_tag):
                        yield data[:-1 * len(self.end_tag)]
                        cont = 0
                    else:
                        yield data
        finally:
            loop.remove_reader(self._p[0])
            if clean_writer:
                loop.remove_writer(self.data[1])
            if not f.done():
                f.cancel()
            process_executor.shutdown(False, cancel_futures=True)
            if exc := f.exception():
                raise exc

    async def decompress(
        self, loop: asyncio.AbstractEventLoop, max_length=-1
    ) -> AsyncGenerator[Union[bytes, tuple]]:
        clean_writer = False
        if isinstance(self.data, SharedMemory):
            args = (
                self.compress_module,
                self.data.name, self._p[1], self.end_tag
            )
        else:
            args = (
                self.compress_module,
                self.data[0], self._p[1], self.end_tag
            )

            loop.add_writer(self.data[1], self._write_data1)
            clean_writer = True

        func = partial(
            _incremental_decompress, *args,
            max_length=max_length, blksize=self.blksize
        )
        read_q = asyncio.Queue()

        def _read():
            dl = os.read(self._p[0], 2)
            d = os.read(self._p[0], int.from_bytes(dl, sys.byteorder))
            read_q.put_nowait(d)

        loop.add_reader(self._p[0], _read)
        process_executor = ProcessPoolExecutor()
        f = loop.run_in_executor(process_executor, func)
        try:
            await asyncio.sleep(0)
            cont = 1
            read_num = 0
            while cont:
                if self._remote_exception:
                    remote_exception = msg_unpack(self._remote_exception)
                    raise RemoteException(remote_exception)
                try:
                    data = await asyncio.wait_for(read_q.get(), 1)
                    read_q.task_done()
                except asyncio.TimeoutError:
                    data = b""
                if data:
                    read_num += 1
                    if data.endswith(self.end_tag):
                        yield data[:-1 * len(self.end_tag)]
                        cont = 0
                    else:
                        yield data
                await asyncio.sleep(0)
        finally:
            loop.remove_reader(self._p[0])
            if clean_writer:
                loop.remove_writer(self.data[1])
            if not f.done():
                f.cancel()
            process_executor.shutdown(False, cancel_futures=True)
            if exc := f.exception():
                raise exc


@overload
def interface(method_or_executor: Callable):
    """
    :param method_or_executor: 给暴露出去的方法添加属性
    """


@overload
def interface(method_or_executor: str):
    """
    :param method_or_executor: 当方法为同步方法时，用于执行的执行器偏好，
      可选 thread 或 process 或 none，none 表示不使用执行器
    """


def interface(method_or_executor):
    if inspect.isfunction(method_or_executor):
        method_or_executor.__interface__ = True
        method_or_executor.__executor__ = "none"
        return method_or_executor
    elif (
            isinstance(method_or_executor, str)
            and method_or_executor in ("thread", "process", "none")
    ):
        if inspect.iscoroutinefunction(method_or_executor):
            raise RuntimeError

        def wrapper(methode):
            methode.__interface__ = True
            methode.__executor__ = method_or_executor
            return methode

        return wrapper
    raise TypeError(method_or_executor.__class__)


def run_in_executor(loop, executor, func, *args, **kwargs) -> asyncio.Future:
    func_call = functools.partial(func, *args, **kwargs)
    return loop.run_in_executor(executor, func_call)
