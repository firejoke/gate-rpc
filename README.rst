gate-rpc
############

使用 ZeroMQ 和 asyncio 开发的“gate-rpc”。

- 使用msgpack序列化消息
- 支持在线程池和进程池里运行普通函数
- 当函数返回的是 Generator 或 AsyncGenerator 时，会换成流式传输，接收端通过遍历 StreamReply 实例即可获得
- 当函数返回的值过大时，会转换成 HugeData 对象，压缩返回值并分块流式传输，默认块大小通过 Settings 的 HUGE_DATA_SIZEOF 来设置
- 使用队列传输和记录日志，避免因为日志导致事件循环被阻塞
- 使用可以异步设置键值和异步获取键值的 BoundedDict 来简化超时等待获取
- 在每次实例化 Worker、Server、AMajordomo、Client 各类之前，通过修改 Settings 的属性来修改运行配置

********
配置
********
在实例化 Worker、Service、AMajordomo、Client 各类之前，需要运行 Settings.setup 函数来配置全局配置

::

    # 可能会修改的几个主要配置
    Settings.MESSAGE_MAX = Worker 和 Client 实例里等待处理的消息最大数量
    Settings.HUGE_DATA_SIZEOF = 每次传输的结果值的最大大小，超过该值的将会被压缩并分片传输
    Settings.SERVICE_DEFAULT_NAME = 默认的服务名，当在实例化 Service 时如果不提供 name 参数则会以这个为服务名
    Settings.MDP_INTERNAL_SERVICE_PREFIX = MDP 内部服务的前缀
    Settings.MDP_HEARTBEAT_INTERVAL = 服务端和客户端相对于中间代理的心跳间隔时间
    Settings.MDP_HEARTBEAT_LIVENESS = 判定掉线的丢失心跳次数，即当超过该次数*心跳时间没有收到心跳则认为已经掉线
    Settings.REPLY_TIMEOUT = 客户端调用远程方法时，等待回复的超时时间，应设置的远远大于心跳时间，默认是一分钟
    Settings.setup()

********
测试示范
********

继承Worker类，用interface装饰希望被远程调用的方法，然后实例化一个Server来创建Worker的实例，这个worker实例的描述信息由server实例提供。

::

    # Worker
    class GRWorker(Worker):
        @interface
        async def atest(self, *args, **kwargs):
            loop = self._get_loop()
            return {
                "name": "async atest",
                "args": args,
                "kwargs": kwargs,
                "loop_time": loop.time()
            }

        @interface
        def test(self, *args, **kwargs):
            return {
                "name": "test",
                "args": args,
                "kwargs": kwargs,
                "loop_time": time()
            }

        @interface
        def test_generator(self, maximum: int):
            i = 0
            while i < maximum:
                yield i
                i += 1

        @interface
        async def test_agenerator(self, maximum: int):
            i = 0
            while i < maximum:
                await asyncio.sleep(0.1)
                yield i
                i += 1

    Settings.setup()
    gr = Service(name="SRkv")
    gr_worker = gr.create_worker(GRWorker, "tcp://127.0.0.1:5555")
    gr_worker.run()

当要执行 IO 密集或 CPU 密集型操作时，可以在方法内使用执行器来执行，可以使用自带的两个执行器，也可以使用自定义的；
另，所有同步的函数都会使用默认执行器执行，默认执行器是 ThreadPoolExecutor 实例，可以修改。

::

    @interface
    async def test_io():
        result = await self.run_in_executor(self.thread_executor, func, *args, **kwargs)
        return result

    @interface
    async def test_cpu():
        # 如果需要和 CPU 密集型执行器里的方法交换数据，可以使用 utils.SyncManager 来创建代理对象使用。
        queue = SyncManager.Queue()
        result = await self.run_in_executor(self.process_executor, func, queue, *args, **kwargs)
        return result

实例化代理时会绑定两个地址，一个用于给后端服务连接上来，一个给前端客户端连接上来。

::

    # Majordomo
    Settings.setup()
    gr_majordomo = AMajordomo(backend_addr="tcp://127.0.0.1:5555")
    gr_majordomo.bind("tcp://127.0.0.1:777")
    gr_majordomo.run()

客户端直接连接代理地址，使用点语法调用远程方法，一般格式是 client.服务名.方法名，当直接使用 client.方法名时，会使用默认服务名调用。

::

    # Client
    Settings.setup()
    gr_cli = Client(broker_addr="tcp://127.0.0.1:777")
    await gr_cli.SRkv.test("a", "b", "c", time=time())
    await gr_cli.SRkv.atest("a", "b", "c", time=time())
    async for i in await gr_cli.SRkv.test_agenerator(10):
        print(i)
