# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/29 19:12
import asyncio
import random
import unittest

from gaterpc.utils import BoundedDict


class TestBoundedDict(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.bd = BoundedDict(dict(), 1000, 3)
        super().__init__(*args, **kwargs)

    async def _aset(self):
        i = 0
        while 1:
            print(f"set value of {i}")
            await self.bd.aset(i, f"{i} value")
            i += 1
            # await asyncio.sleep(0)

    async def _aget(self):
        i = 0
        while 1:
            print(f"get value of {i}")
            v = await self.bd.aget(i)
            print(f'value of {i} is: "{v}"')
            i += 1

    async def one_aset(self, key, value):
        await asyncio.sleep(4)
        print("sleep end")
        await self.bd.aset(key, value)
        # self.bd[key] = value
        print(f"set {key} success")

    async def one_aget(self, key, i, ignore=True):
        await asyncio.sleep(random.uniform(1.0, 1.5))
        try:
            print(f"{i} start get.")
            v = await self.bd.aget(key, ignore=ignore)
            # v = await self.bd[key]
            print(f"{i}: {key}: {v}")
        except asyncio.CancelledError:
            v = None
        return v

    async def _async_tasks(self):
        k = "a"
        v = 10
        l = [self.one_aget(k, i) for i in range(9)]
        # l.append(self.one_aget(k, 9, ignore=False))
        l.append(self.one_aset(k, v))
        l = await asyncio.gather(*l)
        print(f"async tasks result: {l}")

    def test_a(self):
        print(self.bd)
        asyncio.run(self._async_tasks())


if __name__ == '__main__':
    unittest.main()
