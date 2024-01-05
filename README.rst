gate-rpc
############

使用 ZeroMQ 和 asyncio 开发的”gate-rpc”。

- 使用msgpack序列化消息
- 支持在线程池和进程池里运行普通函数
- 当函数返回的是 Generator 或 AsyncGenerator 时，会换成流式传输，接收端通过遍历 StreamReply 实例即可获得
- 使用队列传输和记录日志，避免因为日志导致事件循环被阻塞
- 使用可以异步设置键值和异步获取键值的 BoundedDict 来简化超时等待获取

--------
测试示范
--------
::

    import asyncio
    import sys
    from collections.abc import AsyncGenerator, Generator
    from logging import getLogger
    from time import time
    from traceback import format_exception

    from gaterpc.global_settings import Settings
    from gaterpc.core import Worker, Service, AMajordomo, Client

    logger = getLogger("commands")


    class GRWorker(Worker):
        pass


    class GRService(Service):
        pass


    class GRMajordomo(AMajordomo):
        pass


    class SRClient(Client):
        pass


    async def test():
        # loop = asyncio.get_running_loop()
        # loop.set_debug(True)
        Settings.DEBUG = True
        Settings.setup()
        gr_majordomo = GRMajordomo(backend_addr="tcp://127.0.0.1:5555")
        gr_majordomo.bind("tcp://127.0.0.1:777")
        gr_majordomo.run()
        gr = GRService(name="SRkv")
        gr_worker = gr.create_worker(
           GRWorker, "tcp://127.0.0.1:5555"
        )
        logger.info(gr_worker.service)
        if gr_worker.service is not gr:
            return
        logger.info(gr_worker.interfaces)
        gr_worker.run()
        await asyncio.sleep(5)
        gr_cli = SRClient("tcp://127.0.0.1:777")
        i = 100
        logger.info("start test")
        try:
            while i:
                logger.info(f"i: {i}\n")
                result = await gr_cli.SRkv.test("a", "b", "c", time=time())
                logger.info(f"test: {result}")
                result = await gr_cli.SRkv.atest("d", "e", "f", time=time())
                logger.info(f"atest: {result}")
                i -= 1
            agen = await gr_cli.SRkv.test_agenerator(10)
            logger.info(f"is async generator: {isinstance(agen, AsyncGenerator)}")
            async for i in agen:
                logger.info(f"get i for agen: {i}")
        except Exception as e:
            for line in format_exception(*sys.exc_info()):
                logger.error(line)
            gr_cli.close()
            await gr_worker.stop()
            gr_majordomo.stop()
            raise e


    if __name__ == "__main__":
        asyncio.run(test(), debug=True)
