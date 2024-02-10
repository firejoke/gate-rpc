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
import logging
import lzma
import platform
import queue
import struct
import sys
import threading
import time
import warnings
import zlib

from asyncio import Future, Task
from collections import UserDict, deque
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import wraps
from importlib import import_module
from logging import Handler, getLogger
from multiprocessing import Manager
from multiprocessing.managers import (
    ArrayProxy, DictProxy, ListProxy, ValueProxy,
)
from multiprocessing.shared_memory import SharedMemory
from traceback import format_exception, format_tb
from typing import (
    Any, Dict, Iterable, MutableMapping, Optional, TypeVar,
    Union, overload, ByteString,
)

import msgpack

from .exceptions import BadGzip, DictFull, RemoteException
from .global_settings import Settings
from .mixins import _LoopBoundMixin


SyncManager = Manager()
KT = TypeVar("KT")
VT = TypeVar("VT")


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


class Temp:
    pass


class BoundedDict(UserDict, _LoopBoundMixin):
    """
    参考 asyncio.BoundedSemaphore
    有大小限制和超时获取的字典类，可存储键值对的最大数量默认为1000，
    建议根据内存大小和可能接收的消息的大小设置
    """

    def __init__(
        self, maxsize: int = 1000, timeout: float = 0,
        /, **kwargs
    ):
        """
        :param maxsize: 键值对最大数量。
        :param timeout: 任何异步操作的超时时间，单位是秒。
        :param kwargs: 初始键值对。
        """
        self.maxsize = maxsize
        if self.maxsize <= 0:
            self.maxsize = float("+inf")
        self.usable_size = maxsize
        self.timeout = timeout
        self._waiters: dict[KT, tuple[Future, deque[Future]]] = dict()
        self._later_task: set = set()
        self._sync_task = deque()
        data = kwargs
        if (len(data) + len(kwargs)) > maxsize:
            raise RuntimeError(
                "The initial data size exceeds the maximum size limit."
            )
        super().__init__(data, **kwargs)

    def _set_locked(self):
        return self.usable_size == 0

    def full(self):
        return self._set_locked()

    async def aset(self, key: KT, value: VT, timeout: float = None):
        if timeout is None:
            timeout = self.timeout
        fut, waiters = self._waiters.get(key, (None, None))
        if not fut and not self._set_locked():
            if key not in self.data:
                self.usable_size -= 1
            self.data[key] = value
            return
        elif fut and not self._set_locked():
            if not fut.done():
                fut.set_result(None)
        elif not fut:
            fut = self._get_loop().create_future()
            waiters: deque[Future] = deque()
            self._waiters[key] = (fut, waiters)
        try:
            if not fut.done():
                await asyncio.wait_for(fut, timeout)
            self.data[key] = value
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(True)
        except asyncio.CancelledError:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
            self.usable_size += 1
            self._wake_up_next_set()
            raise
        except asyncio.TimeoutError:
            raise DictFull(self.maxsize)
        finally:
            self._waiters.pop(key)
            self._wake_up_next_set()
        return

    def set(self, key: KT, value: VT):
        sync_task = self._get_loop().create_task(self.aset(key, value))
        self._sync_task.append(sync_task)
        sync_task.add_done_callback(self._sync_task.remove)
        return sync_task

    def _release_set(self):
        if self.usable_size >= self.maxsize:
            return
            # raise ValueError('BoundedDict released too many times')
        self.usable_size += 1
        self._wake_up_next_set()

    def _wake_up_next_set(self):
        for key, (fut, waiters) in self._waiters.items():
            if not fut.done():
                self.usable_size -= 1
                fut.set_result(True)
            return

    async def apop(
        self,
        key: KT,
        default: VT = Temp,
        timeout: float = None,
    ):
        """
        :param key: 要弹出的 key
        :param default: 没有 key 时返回的值
        :param timeout: 超时时间
        :return:
        """
        if timeout is None:
            timeout = self.timeout

        try:
            value = self.data.pop(key)
            return value
        except KeyError:
            if self.full():
                if default != Temp:
                    return default
                raise
            if key in self._waiters and self._waiters[key][0].done():
                raise RuntimeWarning('wait for the "set" lock to be released.')
            if key not in self._waiters:
                self._waiters[key] = (self._get_loop().create_future(), deque())
            fut = self._get_loop().create_future()
            dq = self._waiters[key][1]
            self._waiters[key][1].append(fut)
            try:
                await asyncio.wait_for(fut, timeout)
                try:
                    return self.data.pop(key)
                except KeyError:
                    if default != Temp:
                        return default
                    raise
            except asyncio.TimeoutError:
                if default != Temp:
                    return default
                raise KeyError
            finally:
                dq.remove(fut)

    async def aget(
        self, key: KT,
        default: VT = Temp,
        timeout: float = None,
    ) -> VT:
        """
        :param key: 要查询的 key
        :param default: 没有 key 时返回的值
        :param timeout: 超时时间
        :return:
        """
        if timeout is None:
            timeout = self.timeout

        try:
            value = self.data[key]
            return value
        except KeyError:
            if self.full():
                if default != Temp:
                    return default
                raise
            if key in self._waiters and self._waiters[key][0].done():
                raise RuntimeWarning('wait for the "set" lock to be released.')
            if key not in self._waiters:
                self._waiters[key] = (self._get_loop().create_future(), deque())
            fut = self._get_loop().create_future()
            dq = self._waiters[key][1]
            dq.append(fut)
            try:
                await asyncio.wait_for(fut, timeout)
                try:
                    return self.data[key]
                except KeyError:
                    if default != Temp:
                        return default
                    raise
            except asyncio.TimeoutError:
                if default != Temp:
                    return default
                raise KeyError
            finally:
                dq.remove(fut)

    def __getitem__(self, key) -> VT:
        return self.data[key]

    def __setitem__(self, key: KT, value: VT) -> None:
        if not self._set_locked():
            if key not in self.data:
                self.usable_size -= 1
            self.data[key] = value
        else:
            raise RuntimeWarning('wait for the "set" lock to be released.')

    def __delitem__(self, key: KT) -> None:
        del self.data[key]
        return self._release_set()

    def popitem(self) -> tuple[KT, VT]:
        k, v = super().popitem()
        return k, v

    def pop(self, key: KT, default=Temp) -> VT:
        if default == Temp:
            value = super().pop(key)
        else:
            value = super().pop(key, default=default)
        return value

    @overload
    def update(self, m: MutableMapping[KT, VT], **kwargs: VT) -> None:
        pass

    @overload
    def update(self, m: Iterable[tuple[KT, VT]], **kwargs: VT) -> None:
        pass

    def update(self, m: None, **kwargs: VT) -> None:
        if isinstance(m, MutableMapping):
            items = m.items()
        elif isinstance(m, Iterable):
            items = m
        else:
            items = []
        for k, v in items:
            self.set(key=k, value=v)
        for k, v in kwargs.items():
            self.set(key=k, value=v)
        return


class AQueueListener(object):
    _sentinel = None

    def __init__(
        self, loop, handlers: Union[list[Handler], tuple[Handler]] = None,
        respect_handler_level=True
    ):
        self.respect_handler_level = respect_handler_level
        if isinstance(loop, str):
            loop = resolve_module_attr(loop)
        if isinstance(loop, Callable):
            loop = loop()
        self._loop: asyncio.AbstractEventLoop = loop
        self.queue = queue.Queue()
        self.handlers = dict()
        self._task = None
        self._thread = None
        if handlers:
            self.add_handlers(handlers)
        atexit.register(self.stop)
        # self._loop.call_soon(self.start)
        self.start()

    def add_handlers(self, handlers: Union[list[Handler], tuple[Handler]]):
        for handler in handlers:
            self.handlers[handler.get_name()] = handler

    def dequeue(self, block=True):
        return self.queue.get(block=block)

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
                record = self.dequeue(True)
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


class QueueHandler(Handler):
    def __init__(
        self, handler_class: Union[Handler, str], *args, **kwargs
    ):
        Handler.__init__(self)
        if isinstance(handler_class, str):
            handler_class = resolve_module_attr(handler_class)
        self.listener: Optional[AQueueListener] = None
        self.handler = handler_class(*args, **kwargs)
        self.h_name = f"QueueHandler-handler"

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


class AQueueHandler(QueueHandler):
    def __init__(
        self, *args, handler_class: Union[Handler, str],
        loop, **kwargs
    ):
        super().__init__(handler_class, *args, **kwargs)
        self._name = f"AQueueHandler{id(self.handler)}"
        self.h_name = f"{self._name}-handler"
        self.handler.set_name(self.h_name)
        self.listener = singleton_aqueue_listener(loop)
        self.listener.add_handlers((self.handler,))


singleton_aqueue_listener = singleton(AQueueListener)


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
    def __init__(self, maxsize=0, timeout=0):
        loop = self._get_loop()
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
        if value == Settings.STREAM_END_MESSAGE:
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
                value = __type(value)
            else:
                value = __type()
            await self.replies.put(__type(value))

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
    end_tag = b"HugeDataEND"
    except_tag = b"HugeDataException"

    def __init__(
        self, *,
        data: Optional[bytes] = None,
        get_timeout: int = 0,
        compress_module: str = Settings.HUGE_DATA_COMPRESS_MODULE,
        compress_level: int = Settings.HUGE_DATA_COMPRESS_LEVEL
    ):
        if data:
            self.data: SharedMemory = SharedMemory(create=True, size=len(data))
            self.data.buf[:len(data)] = data
        else:
            self.data: queue.Queue = SyncManager.Queue()
        self.get_timeout = get_timeout
        self._queue: queue.Queue = SyncManager.Queue()
        self.compress_module = compress_module
        self.compress_level = compress_level

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
                        buffer[:Settings.HUGE_DATA_SIZEOF],
                        buffer[Settings.HUGE_DATA_SIZEOF:]
                    )
                    self._queue.put(data)
            buffer = compressor.flush()
            while buffer:
                data, buffer = (
                    buffer[:Settings.HUGE_DATA_SIZEOF],
                    buffer[Settings.HUGE_DATA_SIZEOF:]
                )
                self._queue.put(data)
        except Exception as error:
            except_info = (
                    str(error.__class__),
                    str(error),
                    "".join(format_tb(error.__traceback__))
                )
            logging.error("\n".join(except_info))
            self._queue.put(HugeData.except_tag)
            self._queue.put(except_info)
        finally:
            self._queue.put(self.end_tag)

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
                           (shm_index := shm_index + Settings.HUGE_DATA_SIZEOF)
                           ]
                else:
                    data = self.data.get()
                    self.data.task_done()
                if throw:
                    exception = msg_unpack(data)
                    raise RemoteException(*exception)
                if data == self.end_tag:
                    raise OSError("The compressed data is incomplete")
                if data == self.except_tag:
                    throw = True
                    continue
                data = decompressor.decompress(data, max_length)
                self._queue.put(data)
        except Exception as error:
            except_info = (
                str(error.__class__),
                str(error),
                "".join(format_tb(error.__traceback__))
            )
            self._queue.put(HugeData.except_tag)
            self._queue.put(except_info)
        finally:
            self._queue.put(self.end_tag)

    async def _data_agenerator(
        self,
        func, *args, **kwargs
    ) -> AsyncGenerator[Union[bytes, tuple]]:
        loop = asyncio.get_event_loop()
        func = functools.partial(func, *args, **kwargs)
        with ProcessPoolExecutor() as executor:
            f = loop.run_in_executor(executor, func)
            while 1:
                if self._queue.empty():
                    await asyncio.sleep(1)
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
    @wraps(methode)
    def wrapper(*args, **kwargs):
        return methode(*args, **kwargs)

    @wraps(methode)
    async def awrapper(*args, **kwargs):
        return await methode(*args, **kwargs)

    wrapper.__interface__ = True
    awrapper.__interface__ = True
    if inspect.iscoroutinefunction(methode):
        return awrapper
    return wrapper


async def generator_to_agenerator(generator: Generator) -> AsyncGenerator:
    for element in generator:
        yield element


async def generate_reply(
    task: Task = None, result: Any = None, exception: Exception = None
):
    if task:
        try:
            await task
            result = task.result()
        except Exception as error:
            result = None
            exception = error
    if exception:
        exception = (
            str(exception.__class__),
            str(exception),
            "".join(format_tb(exception.__traceback__))
        )
    if isinstance(result, Generator):
        result = generator_to_agenerator(result)
    if isinstance(result, (AsyncGenerator, HugeData)):
        return result
    if sys.getsizeof(
            result, Settings.HUGE_DATA_SIZEOF
    ) > Settings.HUGE_DATA_SIZEOF:
        loop = asyncio.get_event_loop()
        with ProcessPoolExecutor() as executor:
            _msg_pack = functools.partial(
                msg_pack, result
            )
            data = await loop.run_in_executor(executor, _msg_pack)
        return HugeData(data=data)
    else:
        return {
            "result": result,
            "exception": exception
        }


def check_zap_args(mechanism: Optional[str], credentials: Optional[tuple]):
    if not mechanism:
        return mechanism, credentials
    mechanism = mechanism.encode("utf-8")
    if credentials is None:
        credentials = tuple()
    if (mechanism == Settings.ZAP_MECHANISM_NULL
            and len(credentials) != 0):
        AttributeError(
            f'The "{Settings.ZAP_MECHANISM_NULL}" mechanism '
            f'should not have credential frames.'
        )
    elif (mechanism == Settings.ZAP_MECHANISM_PLAIN
          and len(credentials) != 2):
        AttributeError(
            f'The "{Settings.ZAP_MECHANISM_PLAIN}" mechanism '
            f'should have tow credential frames: '
            f'a username and a password.'
        )
    elif mechanism == Settings.ZAP_MECHANISM_CURVE:
        raise RuntimeError(
            f'The "{Settings.ZAP_MECHANISM_CURVE}"'
            f' mechanism is not implemented yet.'
        )
    elif mechanism not in (
            Settings.ZAP_MECHANISM_NULL,
            Settings.ZAP_MECHANISM_PLAIN,
            Settings.ZAP_MECHANISM_CURVE
    ):
        raise ValueError(
            f'mechanism can only be '
            f'"{Settings.ZAP_MECHANISM_NULL}" or '
            f'"{Settings.ZAP_MECHANISM_PLAIN}" or '
            f'"{Settings.ZAP_MECHANISM_CURVE}"'
        )
    return mechanism, credentials
