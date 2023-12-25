# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/22 16:36
import asyncio
import platform
import struct
from asyncio import Task
from collections import UserDict, deque
from copy import deepcopy
from functools import wraps
from logging import getLogger
from typing import (
    Any, Dict, Iterable, MutableMapping, Optional, TypeVar,
    Union, overload, ByteString,
)

from .mixins import _LoopBoundMixin
from gaterpc.conf.global_settings import Settings


KT = TypeVar("KT")
VT = TypeVar("VT")


logger = getLogger(__name__)


class BoundedDict(UserDict, _LoopBoundMixin):
    """
    参考 asyncio.BoundedSemaphore
    有大小限制和超时获取的字典类，可存储键值对的最大数量默认为1000，
    建议根据内存大小和可能接收的消息的大小设置
    """

    def __init__(
        self, data: dict = None, maxsize: int = 1000, timeout: int = 0,
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
        self._set_waiters: deque[asyncio.Future] = deque()
        self._get_waiters: Dict[KT, deque[asyncio.Future]] = dict()
        if (len(data) + len(kwargs)) > maxsize:
            raise RuntimeError(
                "The initial data size exceeds the maximum size limit."
            )
        super().__init__(data, **kwargs)

    def _set_locked(self):
        return self.usable_size == 0 or (
            any(not w.cancelled() for w in self._set_waiters)
        )

    async def aset(self, key: KT, value: VT, timeout: int = None):
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
                    async with asyncio.timeout(timeout):
                        await fut
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
        return asyncio.run(self.aset(key, value, timeout=None))

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
        timeout: int = None,
        ignore: bool = True
    ) -> VT:
        """
        :param key: 要查询的 key
        :param default: 没有 key 时返回的值
        :param timeout: 超时时间
        :param ignore: 没有 key 时是否引发索引错误
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
                    async with asyncio.timeout(timeout):
                        await fut
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
            raise
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
        if isinstance(m, MutableMapping):
            items = m.items()
        elif isinstance(m, Iterable):
            items = m
        else:
            items = []
        for k, v in items:
            asyncio.run(self.aset(key=k, value=v))
        for k, v in kwargs.items():
            asyncio.run(self.aset(key=k, value=v))
        return


def to_bytes(s: Union[str, bytes, float, int]):
    if isinstance(s, str):
        return s.encode("utf-8")
    elif isinstance(s, int):
        if -128 <= s <= 127:
            return struct.pack("!b", s)
        elif -32768 <= s <= 32767:
            return struct.pack("!h", s)
        elif -2147483648 <= s <= 2147483647:
            return struct.pack("!i", s)
        return struct.pack("!d", s)
    elif isinstance(s, float):
        return struct.pack("!d", s)
    return s


def from_bytes(b: bytes) -> Union[str, float, int]:
    fmt = {
        1: "!b",
        2: "!h",
        4: "!i",
        8: "!d"
    }
    if b_len := len(b) in fmt:
        try:
            return struct.unpack(fmt[b_len], b)
        except struct.error:
            pass
    return b.decode("utf-8")


def msg_dump(obj: Any) -> ByteString:
    """
    TODO: python对象序列化为数据
    """
    data = obj
    return data


def msg_load(data: ByteString) -> Any:
    """
    TODO: 数据反序列化为python对象
    """
    obj = data
    return obj


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


async def gen_reply(
    task: Task = None, result: Any = None, exception: Exception = None
):
    reply = deepcopy(Settings.RPC_REPLY_TEMPLATE)
    if task:
        try:
            await task
            reply["result"] = task.result()
            reply["exception"] = task.exception()
        except asyncio.CancelledError as error:
            reply["result"] = None
            reply["exception"] = error
    else:
        reply["result"] = result
        reply["exception"] = exception
    return reply


def interface(methode):
    @wraps(methode)
    def wrapper(*args, **kwargs):
        logger.debug(
            f"method: {methode.__name__}, args: {args}, kwargs: {kwargs}"
        )
        return methode(*args, **kwargs)
    wrapper.__interface__ = True
    return wrapper
