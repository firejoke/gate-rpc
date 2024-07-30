# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/22 16:36
import asyncio
import atexit
import bz2
import copy
import inspect
import io
import logging
import lzma
import operator
import platform
import queue
import struct
import threading
import time
import warnings
from pathlib import Path

import math
import zlib

from asyncio import Future, Task
from collections import Counter, UserDict, deque, namedtuple
from collections.abc import AsyncGenerator, Callable, Generator
from concurrent.futures import ProcessPoolExecutor
from functools import (
    WRAPPER_ASSIGNMENTS, _make_key, partial, update_wrapper,
    wraps, lru_cache,
)
from importlib import import_module
from logging import Handler
from multiprocessing import Manager
from multiprocessing.managers import (
    ArrayProxy, DictProxy, ListProxy, ValueProxy,
)
from multiprocessing.shared_memory import SharedMemory
from traceback import format_tb
from typing import (
    Any, Iterable, MutableMapping, Optional, TypeVar,
    Union, overload, ByteString,
)

import msgpack

from .exceptions import BadGzip, DictFull, HugeDataException, RemoteException
from .mixins import _LoopBoundMixin


SyncManager = Manager()


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


class LRUCache(object):
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self.hits = self.misses = 0
        self.full = False
        self.root = []
        self.root[:] = [self.root, self.root, None, None]
        self.cache = {}

    def __contains__(self, item):
        if item in self.cache:
            self.hits += 1
            return True
        self.misses += 1
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
        render: Optional[Callable[[_I, _PV], _PV]] = None,
        process: Optional[Callable[[_I, _PV], _PV]] = None,

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
        # self._loop.call_soon(self.start)
        self.start()

    def add_handlers(self, handlers: Union[list[Handler], tuple[Handler]]):
        for handler in handlers:
            self.handlers[handler.get_name()] = handler

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
        if not (loop := asyncio.get_event_loop()).is_running():
            self._loop = loop
            # self._loop.run_forever()
        else:
            self._loop = None
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
        if self._loop:
            self._task = self._loop.create_task(self._monitor())
        else:
            self._task = asyncio.create_task(self._monitor())
        # self._thread = t = threading.Thread(target=self._monitor)
        # t.daemon = True
        # t.start()

    def stop(self):
        if self._task:
            self._task.cancel()
        if self._loop:
            self._loop.stop()
        # if self._thread:
        #     self.enqueue_sentinel()
        #     self._thread.join()
        #     self._thread = None


class AQueueHandler(Handler):
    def __init__(
        self, handler_class: Union[Handler, str], *args, **kwargs
    ):
        Handler.__init__(self)
        if isinstance(handler_class, str):
            handler_class = resolve_module_attr(handler_class)
        self.handler = handler_class(*args, **kwargs)
        if asyncio.get_event_loop().is_running():
            self._name = f"AQueueHandler{id(self.handler)}"
            self.listener = singleton_aqueue_listener()
        else:
            self._name = f"QueueHandler{id(self.handler)}"
            self.listener = singleton_queue_listener()
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
            self.listener.stop()
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
        ) and value and not callable(value):
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


class StreamReply(AsyncGenerator, _LoopBoundMixin):
    def __init__(self, end_message, maxsize=0, timeout=0):
        loop = self._get_loop()
        self.end_message = end_message
        self.replies = asyncio.Queue(maxsize, loop=loop)
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
                self.replies.get(), self.timeout, loop=self._get_loop()
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
            # Read & discard the extra field, if present
            extra_len, = struct.unpack(
                "<H", data[index: (index := index + 2)]
            )
            index += extra_len
        if flag & 8:
            # Read and discard a null-terminated string containing the filename
            while True:
                s = data[(index := index + 1)]
                if not s or s == b'\000':
                    break
        if flag & 16:
            # Read and discard a null-terminated string containing a comment
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
        crc32, isize = struct.unpack("<II", self._decompressor.unused_data[:8])
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
        if not self.needs_input:
            data = self._decompressor.unconsumed_tail + data
        data = self._decompressor.decompress(data, max_length)
        if data == b"":
            raise EOFError("Compressed data ended before the "
                           "end-of-stream marker was reached")
        self._crc = zlib.crc32(data, self._crc)
        self._stream_size += len(data)
        if self._decompressor.eof:
            self._read_eof()
        return data


class HugeData(object):

    def __init__(
        self,
        end_tag,
        except_tag,
        *,
        data: Optional[bytes] = None,
        get_timeout: int = 0,
        compress_module: str = "gzip",
        compress_level: int = 9,
        frame_size_limit: int = 1000
    ):
        self.end_tag = end_tag
        self.except_tag = except_tag
        self.exception: Optional[tuple[str, str, list]] = None
        self.data: Union[SharedMemory, queue.Queue]
        if data:
            self.data = SharedMemory(create=True, size=len(data))
            self.data.buf[:len(data)] = data
        else:
            self.data = SyncManager.Queue()
        self.get_timeout = get_timeout
        self._queue: queue.Queue = SyncManager.Queue()
        self.compress_module = compress_module
        self.compress_level = compress_level
        self.frame_size_limit = frame_size_limit

        atexit.register(self.destroy)

    def destroy(self):
        if isinstance(self.data, SharedMemory):
            if self.data.buf is not None:
                self.data.buf.release()
            self.data.close()
            try:
                self.data.unlink()
            except (FileNotFoundError, ImportError):
                pass
        atexit.unregister(self.destroy)

    def __del__(self):
        self.destroy()

    def _incremental_compress(self):
        try:
            if self.compress_module == "gzip":
                compressor = GzipCompressor(self.compress_level)
            elif self.compress_module == "bz2":
                compressor = bz2.BZ2Compressor(self.compress_level)
            elif self.compress_module == "lzma":
                compressor = lzma.LZMACompressor(preset=self.compress_level)
            else:
                compress_module = resolve_module_attr(self.compress_module)
                compressor = compress_module.compressor(self.compress_level)
            buffer = b""
            stream_end = False
            shm_index = 0
            while 1:
                if stream_end and not buffer:
                    break
                elif not stream_end:
                    if isinstance(self.data, SharedMemory):
                        data = self.data.buf[
                               shm_index:
                               (shm_index := shm_index + io.DEFAULT_BUFFER_SIZE)
                               ]
                        if not data:
                            stream_end = True
                    else:
                        if self.get_timeout:
                            data = self.data.get(timeout=self.get_timeout)
                        else:
                            data = self.data.get(block=False)
                        if data == self.end_tag:
                            data = b""
                            stream_end = True
                    if data:
                        data = compressor.compress(data)
                        buffer += data
                elif buffer:
                    data, buffer = (
                        buffer[:self.frame_size_limit],
                        buffer[self.frame_size_limit:]
                    )
                    self._queue.put_nowait(data)
            buffer = compressor.flush()
            while buffer:
                data, buffer = (
                    buffer[:self.frame_size_limit],
                    buffer[self.frame_size_limit:]
                )
                self._queue.put_nowait(data)
        except Exception as error:
            raise HugeDataException(error)
        finally:
            self._queue.put_nowait(self.end_tag)

    def _incremental_decompress(self, max_length=-1):
        try:
            if self.compress_module == "gzip":
                decompressor = GzipDecompressor()
            elif self.compress_module == "bz2":
                decompressor = bz2.BZ2Decompressor()
            elif self.compress_module == "lzma":
                decompressor = lzma.LZMADecompressor()
            else:
                compress_module = resolve_module_attr(self.compress_module)
                decompressor = compress_module.decompressor()
            shm_index = 0
            throw = False
            while not decompressor.eof:
                if not decompressor.needs_input:
                    data = b""
                elif isinstance(self.data, SharedMemory):
                    data = self.data.buf[
                           shm_index:
                           (shm_index := shm_index + self.frame_size_limit)
                           ]
                else:
                    data = self.data.get()
                    self.data.task_done()
                if throw:
                    exception = msg_unpack(data)
                    raise RemoteException(exception)
                if data == self.end_tag:
                    raise OSError("The compressed data is incomplete")
                if data == self.except_tag:
                    throw = True
                    continue
                data = decompressor.decompress(data, max_length)
                self._queue.put_nowait(data)
        except Exception as error:
            raise HugeDataException(error)
        finally:
            self._queue.put_nowait(self.end_tag)

    async def _data_agenerator(
        self,
        func, *args, **kwargs
    ) -> AsyncGenerator[Union[bytes, tuple]]:
        loop = asyncio.get_event_loop()
        func = partial(func, *args, **kwargs)
        with ProcessPoolExecutor() as executor:
            f = loop.run_in_executor(executor, func)
            while 1:
                if self._queue.empty():
                    await asyncio.sleep(0.5)
                    continue
                data = self._queue.get()
                self._queue.task_done()
                if data == self.end_tag:
                    break
                yield data
        await f
        if exc := f.exception():
            raise exc

    def compress(self) -> AsyncGenerator[Union[bytes, tuple]]:
        return self._data_agenerator(self._incremental_compress)

    def decompress(self, max_length=-1) -> AsyncGenerator[Union[bytes, tuple]]:
        return self._data_agenerator(self._incremental_decompress, max_length)


def interface(methode):
    if inspect.iscoroutinefunction(methode):
        @wraps(methode)
        async def awrapper(*args, **kwargs):
            return await methode(*args, **kwargs)

        awrapper.__interface__ = True
        return awrapper

    @wraps(methode)
    def wrapper(*args, **kwargs):
        return methode(*args, **kwargs)

    wrapper.__interface__ = True
    return wrapper
