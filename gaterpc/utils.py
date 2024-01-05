# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/22 16:36
import asyncio
import atexit
import bz2
import contextvars
import copy
import functools
import gzip
import inspect
import lzma
import platform
import queue
import struct
import sys
import threading
from asyncio import (
    CancelledError, Task, Queue as AQueue,
    QueueEmpty as AQueueEmpty, events,
)
from collections import UserDict, deque
from collections.abc import AsyncGenerator, Callable, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import wraps
from importlib import import_module
from logging import Handler
from traceback import format_tb
from typing import (
    Any, Dict, Iterable, MutableMapping, Optional, TypeVar,
    Union, overload, ByteString,
)

import msgpack

from .global_settings import Settings
from .mixins import _LoopBoundMixin


KT = TypeVar("KT")
VT = TypeVar("VT")


class BoundedDict(UserDict, _LoopBoundMixin):
    """
    参考 asyncio.BoundedSemaphore
    有大小限制和超时获取的字典类，可存储键值对的最大数量默认为1000，
    建议根据内存大小和可能接收的消息的大小设置
    """

    def __init__(
        self, data: dict = None, maxsize: int = 1000, timeout: float = 0,
        /, **kwargs
    ):
        """
        :param data: 原始字典数据，可为空。
        :param maxsize: 键值对最大数量。
        :param timeout: 任何异步操作的超时时间，单位是秒。
        :param kwargs: 初始键值对。
        """
        self.maxsize = maxsize
        self.usable_size = maxsize
        self.timeout = timeout
        self._set_waiters: deque = deque()
        self._sync_task: set = set()
        self._get_waiters: Dict[KT, deque] = dict()
        if not data:
            data = {}
        if (len(data) + len(kwargs)) > maxsize:
            raise RuntimeError(
                "The initial data size exceeds the maximum size limit."
            )
        super().__init__(data, **kwargs)

    def _set_locked(self):
        return self.usable_size == 0 or (
            any(not w.cancelled() for w in self._set_waiters)
        )

    async def aset(self, key: KT, value: VT, timeout: float = None):
        if not self._set_locked():
            if key not in self.data:
                self.usable_size -= 1
        else:
            if timeout is None:
                timeout = self.timeout
            is_timeout = False
            fut = self._get_loop().create_future()
            self._set_waiters.append(fut)

            try:
                try:
                    await asyncio.wait_for(fut, timeout)
                except asyncio.TimeoutError:
                    is_timeout = True
                finally:
                    self._set_waiters.remove(fut)
            except asyncio.CancelledError:
                if not fut.cancelled():
                    self.usable_size += 1
                    self._wake_up_next_set()
                raise

            if is_timeout:
                raise asyncio.TimeoutError(
                    f"The number of keys reached the maximum of {self.maxsize},"
                    f" and no key was released within {self.timeout} seconds"
                )
            if self.usable_size > 0:
                self._wake_up_next_set()

        self.data[key] = value
        if key in self._get_waiters:
            self._release_get(key)
        return

    def set(self, key: KT, value: VT):
        loop = self._get_loop()
        t = loop.create_task(self.aset(key, value, timeout=None))
        self._sync_task.add(t)
        t.add_done_callback(self._sync_task.remove)

    def _release_set(self):
        if self.usable_size >= self.maxsize:
            raise ValueError('BoundedDict released too many times')
        self.usable_size += 1
        self._wake_up_next_set()

    def _wake_up_next_set(self):
        if not self._set_waiters:
            return

        try:
            fut = self._set_waiters.popleft()
            if not fut.done():
                self.usable_size -= 1
                fut.set_result(True)
        except IndexError:
            pass
        return

    async def aget(
        self, key: KT,
        default: VT = None,
        timeout: float = None,
        ignore: bool = True
    ) -> VT:
        """
        :param key: 要查询的 key
        :param default: 没有 key 时返回的值
        :param timeout: 超时时间
        :param ignore: 没有 key 时是否引发键错误
        :return:
        """
        is_timeout = False
        if timeout is None:
            timeout = self.timeout
        
        if key not in self.data:
            if not (key_waiters := self._get_waiters.get(key)):
                self._get_waiters[key] = key_waiters = deque()
            fut = self._get_loop().create_future()
            key_waiters.append(fut)
            try:
                try:
                    await asyncio.wait_for(fut, timeout)
                except asyncio.TimeoutError:
                    is_timeout = True
                finally:
                    key_waiters.remove(fut)
            except asyncio.CancelledError:
                if not fut.cancelled():
                    fut.set_result(None)
                raise
        try:
            return self.data[key]
        except KeyError:
            if ignore:
                return default
            raise KeyError(key)
        finally:
            if is_timeout and timeout > 0:
                raise asyncio.TimeoutError(f'get "{key}" timeout.')

    def _release_get(self, key):
        if key in self._get_waiters:
            for fut in self._get_waiters[key]:
                if not fut.done():
                    fut.set_result(True)
            self._get_waiters.pop(key)
        return True

    # def get(self, key: KT, default: VT = None, ignore=True) -> Coroutine:
    #     return self.aget(key, default, None, ignore)

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

    def pop(self, key: KT) -> VT:
        value = super().pop(key)
        return value

    @overload
    def update(self, m: MutableMapping[KT, VT], **kwargs: VT) -> None:
        pass

    @overload
    def update(self, m: Iterable[tuple[KT, VT]], **kwargs: VT) -> None:
        pass

    def update(self, m: None, **kwargs: VT) -> None:
        loop = self._get_loop()
        if isinstance(m, MutableMapping):
            items = m.items()
        elif isinstance(m, Iterable):
            items = m
        else:
            items = []
        for k, v in items:
            t = loop.create_task(self.aset(key=k, value=v))
            self._sync_task.add(t)
            t.add_done_callback(self._sync_task.remove)
        for k, v in kwargs.items():
            t = loop.create_task(self.aset(key=k, value=v))
            self._sync_task.add(t)
            t.add_done_callback(self._sync_task.remove)
        return


class StreamReply(AsyncGenerator):
    def __init__(self, maxsize=0, timeout=0):
        self.replies = asyncio.Queue(maxsize)
        self.timeout = timeout
        self._exit = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._exit:
            raise GeneratorExit
        if self.timeout:
            try:
                value = await asyncio.wait_for(self.replies.get(), self.timeout)
            except asyncio.TimeoutError:
                raise StopAsyncIteration
        else:
            try:
                value = self.replies.get_nowait()
            except asyncio.QueueEmpty:
                raise StopAsyncIteration
        self.replies.task_done()
        if value == Settings.STREAM_END_MESSAGE:
            await asyncio.sleep(1)
            raise StopAsyncIteration
        if isinstance(value, BaseException):
            raise value
        return value

    async def asend(self, value):
        if value:
            await self.replies.put(value)
        return

    async def athrow(self, __type: BaseException, value=None, traceback=None):
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


class HugeDataReply(_LoopBoundMixin):
    def __init__(self, compress_module: str, compress_level: int):
        self.compress_module = compress_module
        if self.compress_module == "bz2":
            self._compress = bz2.compress
            self._decompress = bz2.decompress
            self._compress_kwargs = {
                "compresslevel": compress_level
            }
        elif self.compress_module == "lzma":
            self._compress = lzma.compress
            self._decompress = lzma.decompress
            self._compress_kwargs = {
                "preset": compress_level
            }
        else:
            self.compress_module = "gzip"
            self._compress = gzip.compress
            self._decompress = gzip.decompress
            self._compress_kwargs = {
                "compresslevel": compress_level
            }

    async def compress(self, data: bytes) -> bytes:
        loop = self._get_loop()
        ctx = contextvars.copy_context()
        _compress = functools.partial(
            ctx.run, self._compress, data, **self._compress_kwargs
        )
        with ProcessPoolExecutor() as executor:
            data = await loop.run_in_executor(
                executor, _compress
            )
        return data

    async def decompress(self, data: bytes) -> bytes:
        loop = self._get_loop()
        ctx = contextvars.copy_context()
        _decompress = functools.partial(
            ctx.run, self._decompress, data
        )
        with ProcessPoolExecutor() as executor:
            data = await loop.run_in_executor(
                executor, _decompress
            )
        return data


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
        sys.stdout.write(f"AQueueListener.loop: {loop.__class__}\n")
        sys.stdout.flush()
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
                print(e)

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
            print(e)
            self.handleError(record)

    def close(self):
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


def singleton(cls):
    instances = dict()

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances.get(cls)
    return get_instance


singleton_aqueue_listener = singleton(AQueueListener)


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


def msg_pack(obj: Any) -> ByteString:
    data = msgpack.packb(obj)
    return data


def msg_unpack(data: ByteString) -> Any:
    obj = msgpack.unpackb(data)
    return obj


async def gen_reply(
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
    return {
        "result": result,
        "exception": exception
    }
