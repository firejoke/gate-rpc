# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/6/8 11:04
import asyncio
import threading
from asyncio import events


_global_lock = threading.Lock()


class _LoopBoundMixin:
    """
    从 asyncio.mixins 拷贝过来
    """
    _loop = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = events.get_event_loop()

        if self._loop is None:
            with _global_lock:
                if self._loop is None:
                    self._loop = loop
        if loop is not self._loop:
            raise RuntimeError(f'{self!r} is bound to a different event loop')
        return loop
