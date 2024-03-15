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

import math
import zlib

from asyncio import Future, Task
from collections import UserDict, deque, namedtuple
from collections.abc import AsyncGenerator, Callable, Generator
from concurrent.futures import ProcessPoolExecutor
from functools import _make_key, partial, update_wrapper, wraps, lru_cache
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

from .exceptions import BadGzip, DictFull, RemoteException
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


class Temp:

    def __bool__(self):
        return False


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
            # Use the old root to store the new key and result.
            oldroot = self.root
            oldroot[self.KEY] = key
            oldroot[self.RESULT] = value
            # Empty the oldest link and make it the new root.
            # Keep a reference to the old key and old result to
            # prevent their ref counts from going to zero during the
            # update. That will prevent potentially arbitrary object
            # clean-up code (i.e. __del__) from running while we're
            # still adjusting the links.
            self.root = oldroot[self.NEXT]
            oldkey = self.root[self.KEY]
            oldresult = self.root[self.RESULT]
            self.root[self.KEY] = self.root[self.RESULT] = None
            # Now update the cache dictionary.
            del self.cache[oldkey]
            # Save the potentially reentrant cache[key] assignment
            # for last, after the root and links have been put in
            # a consistent state.
            self.cache[key] = oldroot
        else:
            # Put result in a new link at the front of the queue.
            last = self.root[self.PREV]
            link = [last, self.root, key, value]
            last[self.NEXT] = self.root[self.PREV] = self.cache[key] = link
            # Use the cache_len bound method instead of the len() function
            # which could potentially be wrapped in an lru_cache itself.
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
    # Constants shared by all lru cache instances:
    sentinel = object()          # unique object used to signal cache misses
    make_key = _make_key         # build a key from the function arguments
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3   # names for the link fields

    cache = {}
    hits = misses = 0
    full = False
    cache_get = cache.get    # bound method to lookup a key or return None
    cache_len = cache.__len__  # get cache size without calling len()
    lock = threading.RLock()           # because linkedlist updates aren't threadsafe
    root = []                # root of the circular doubly linked list
    root[:] = [root, root, None, None]     # initialize by pointing to self

    if maxsize == 0:

        async def wrapper(*args, **kwds):
            # No caching -- just a statistics update
            nonlocal misses
            misses += 1
            result = await user_function(*args, **kwds)
            return result

    elif maxsize is None:

        async def wrapper(*args, **kwds):
            # Simple caching without ordering or size limit
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
            # Size limited caching that tracks accesses by recency
            nonlocal root, hits, misses, full
            key = make_key(args, kwds, typed)
            with lock:
                link = cache_get(key)
                if link is not None:
                    # Move the link to the front of the circular queue
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
                    # Getting here means that this same key was added to the
                    # cache while the lock was released.  Since the link
                    # update is already done, we need only return the
                    # computed result and update the count of misses.
                    pass
                elif full:
                    # Use the old root to store the new key and result.
                    oldroot = root
                    oldroot[KEY] = key
                    oldroot[RESULT] = f
                    # Empty the oldest link and make it the new root.
                    # Keep a reference to the old key and old result to
                    # prevent their ref counts from going to zero during the
                    # update. That will prevent potentially arbitrary object
                    # clean-up code (i.e. __del__) from running while we're
                    # still adjusting the links.
                    root = oldroot[NEXT]
                    oldkey = root[KEY]
                    oldresult = root[RESULT]
                    root[KEY] = root[RESULT] = None
                    # Now update the cache dictionary.
                    del cache[oldkey]
                    # Save the potentially reentrant cache[key] assignment
                    # for last, after the root and links have been put in
                    # a consistent state.
                    cache[key] = oldroot
                else:
                    # Put result in a new link at the front of the queue.
                    last = root[PREV]
                    link = [last, root, key, f]
                    last[NEXT] = root[PREV] = cache[key] = link
                    # Use the cache_len bound method instead of the len() function
                    # which could potentially be wrapped in an lru_cache itself.
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


class _LazyProperty:
    def __init__(self, _property, ins: Any = None):
        """
        延时计算的属性
        """
        self._property = _property
        self._ins = ins

    def __call__(self, ins: Any = None):
        if not ins:
            ins = self._ins
        if not ins:
            raise RuntimeError
        if callable(self._property):
            return self._property(ins)
        return getattr(ins, self._property)

    def __add__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.add(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.add(self(ins), other),
            self._ins
        )

    def __radd__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.add(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.add(other, self(ins)),
            self._ins
        )

    def __sub__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.sub(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.sub(self(ins), other),
            self._ins
        )

    def __rsub__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.sub(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.sub(other, self(ins)),
            self._ins
        )

    def __mul__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.mul(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.mul(self(ins), other),
            self._ins
        )

    def __rmul__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.mul(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.mul(other, self(ins)),
            self._ins
        )

    def __matmul__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.matmul(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.matmul(self(ins), other),
            self._ins
        )

    def __rmatmul__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.matmul(other(ins), self(ins)),
            )
        return _LazyProperty(
            lambda ins=None: operator.matmul(other, self(ins)),
            self._ins
        )

    def __truediv__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.truediv(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.truediv(self(ins), other),
            self._ins
        )

    def __rtruediv__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.truediv(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.truediv(other, self(ins)),
            self._ins
        )

    def __floordiv__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.floordiv(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.floordiv(self(ins), other),
            self._ins
        )

    def __rfloordiv__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.floordiv(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.floordiv(other, self(ins)),
            self._ins
        )

    def __mod__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.mod(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.mod(self(ins), other),
            self._ins
        )

    def __rmod__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.mod(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.mod(other, self(ins)),
            self._ins
        )

    def __divmod__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: divmod(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: divmod(self(ins), other),
            self._ins
        )

    def __rdivmod__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: divmod(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: divmod(other, self(ins)),
            self._ins
        )

    def __pow__(self, other, modulo=None):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: pow(self(ins), other(ins), modulo),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: pow(self(ins), other, modulo),
            self._ins
        )

    def __rpow__(self, other, modulo=None):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: pow(other(ins), self(ins), modulo),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: pow(other, self(ins), modulo),
            self._ins
        )

    def __lshift__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.lshift(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.lshift(self(ins), other),
        )

    def __rlshift__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.lshift(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.lshift(other, self(ins)),
        )

    def __rshift__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.rshift(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.rshift(self(ins), other),
            self._ins
        )

    def __rrshift__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.rshift(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.rshift(other, self(ins)),
            self._ins
        )

    def __and__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.and_(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.and_(self(ins), other),
            self._ins
        )

    def __rand__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.and_(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.and_(other, self(ins)),
            self._ins
        )

    def __xor__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.xor(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.xor(self(ins), other),
            self._ins
        )

    def __rxor__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.xor(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.xor(other, self(ins)),
            self._ins
        )

    def __or__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.or_(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.or_(self(ins), other),
            self._ins
        )

    def __ror__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.or_(other(ins), self(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.or_(other, self(ins)),
            self._ins
        )

    def __lt__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.lt(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.lt(self(ins), other),
            self._ins
        )

    def __le__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.le(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.le(self(ins), other),
            self._ins
        )

    def __eq__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.eq(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.eq(self(ins), other),
            self._ins
        )

    def __ne__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.ne(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.ne(self(ins), other),
            self._ins
        )

    def __ge__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.ge(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.ge(self(ins), other),
            self._ins
        )

    def __gt__(self, other):
        if isinstance(other, _LazyProperty):
            return _LazyProperty(
                lambda ins=None: operator.gt(self(ins), other(ins)),
                self._ins
            )
        return _LazyProperty(
            lambda ins=None: operator.gt(self(ins), other),
            self._ins
        )

    def __neg__(self):
        return _LazyProperty(
            lambda ins=None: operator.neg(self(ins)),
            self._ins
        )

    def __pos__(self):
        return _LazyProperty(
            lambda ins=None: operator.pos(self(ins)),
            self._ins
        )

    def __abs__(self):
        return _LazyProperty(
            lambda ins=None: operator.abs(self(ins)),
            self._ins
        )

    def __invert__(self):
        return _LazyProperty(
            lambda ins=None: operator.invert(self(ins)),
            self._ins
        )

    def __int__(self):
        return int(self())

    def __float__(self):
        return float(self())

    def __floor__(self):
        return _LazyProperty(
            lambda ins=None: math.floor(self(ins)),
            self._ins
        )

    def __ceil__(self):
        return _LazyProperty(
            lambda ins=None: math.ceil(self(ins)),
            self._ins
        )

    def __trunc__(self):
        return _LazyProperty(
            lambda ins=None: math.trunc(self(ins)),
            self._ins
        )

    def __str__(self):
        return (
            f"Lazy retrieval of the value for instance \"{self._property}\"."
        )


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
        # except Exception as error:
        #     except_info = (
        #             str(error.__class__),
        #             str(error),
        #             "".join(format_tb(error.__traceback__))
        #         )
        #     logging.error("\n".join(except_info))
        #     self._queue.put(HugeData.except_tag)
        #     self._queue.put(except_info)
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
