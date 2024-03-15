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
安装
********

可以直接使用pip安装，或直接下载源码随意放置。

::

    pip install gaterpc


********
配置
********
在实例化 Worker、Service、AMajordomo、Client 各类之前，需要运行 Settings.setup 函数来配置全局配置 [#f1]_ ，
特殊返回值的序列化通过 MessagePack 的全局实例来定制 [#f2]_

::

    # 可能会修改的几个主要配置
    Settings.HEARTBEAT: int = 全局的默认的心跳间隔，单位是毫秒
    Settings.TIMEOUT: float = 全局的默认超时时间，单位是秒
    Settings.MESSAGE_MAX: int = Worker 和 Client 实例里等待处理的消息最大数量
    Settings.HUGE_DATA_SIZEOF: int = 每次传输的结果值的最大大小，超过该值的将会被压缩并分片传输
    Settings.HUGE_DATA_COMPRESS_MODULE: str = 使用的压缩模块的名称 [#f1]_
    Settings.SERVICE_DEFAULT_NAME: str = 默认的服务名，当在实例化 Service 时如果不提供 name 参数则会以这个为服务名
    Settings.WORKER_ADDR: str = Majordomo 用于绑定给 Worker 连接的地址，一般是 inproc 类型，当使用 inproc 地址时，需要和 Majordomo 使用同一个 Context
    Settings.MDP_INTERNAL_SERVICE_PREFIX: bytes = MDP 内部服务的前缀
    Settings.MDP_HEARTBEAT_INTERVAL: int = 服务端和客户端相对于中间代理的心跳间隔时间，单位是毫秒，默认是使用上面的Settings.HEARTBEAT
    Settings.MDP_HEARTBEAT_LIVENESS: int = 心跳活跃度，用于判定掉线的丢失心跳次数，即当超过该次数*心跳时间没有收到心跳则认为已经掉线，默认3次
    Settings.REPLY_TIMEOUT: float = 客户端调用远程方法时，等待回复的超时时间，应设置的远远大于心跳时间，默认是一分钟
    Settings.STREAM_REPLY_MAXSIZE: int = 流式数据使用的缓存队列的最大长度（使用的 asyncio.Queue）
    Settings.REPLY_TIMEOUT: float = 获取回复的超时时间，也是流式传输的每一个子回复的超时时间，单位是秒，默认使用的全局的Settings.TIMEOUT
    Settings.ZAP_PLAIN_DEFAULT_USER: str = ZAP 的 PLAIN 机制的默认用户名
    Settings.ZAP_PLAIN_DEFAULT_PASSWORD: str = ZAP 的 PLAIN 机制的默认密码
    Settings.ZAP_ADDR: str = ZAP 服务绑定的地址，如果是和代理服务一起使用，最好使用 ipc 类型，且不要和代理使用同一个 Context
    Settings.ZAP_REPLY_TIMEOUT: float = 等待 ZAP 服务的回复的超时时间，单位是秒，远比普通的REPLY_TIMEOUT短，因为zap服务处理每一个zap请求必须很快
    Settings.setup()

    # 特殊返回值的序列化配置 [#f2]_
    from gaterpc.utils.message_pack
    message_pack.prepare_pack = 在使用 msgpack.packb 时，传递给 default 参数的可执行对象
    message_pack.unpack_object_hook = 在使用 msgpack.unpackb 时，传递给 object_hook 的可执行对象
    message_pack.unpack_object_pairs_hook = 在使用 msgpack.unpackb 时，传递给 object_pairs_hook 的可执行对象
    message_pack.unpack_object_list_hook = 在使用 msgpack.unpackb 时，传递给 list_hook 的可执行对象


.. rubric:: Footnotes
.. [#f1] Settings.HUGE_DATA_COMPRESS_MODULE 除了内置的 gzip，bz2，lzma，还可以使用外部模块，只要模块提供 compressor 和 decompressor 方法即可，
   compressor 需要返回一个带有 compress 方法的增量压缩器对象，decompressor 需要返回一个带有 decompress 的增量解压缩器对象
.. [#f2] 单一返回值和生成器的元素返回值，以及巨型返回值都会使用 utils.msg_pack 和 utils.msg_unpack 来序列化和反序列化
   这两个方法内部是使用的 utils.MessagePack 的全局实例，如果不能返回常规的“字符串”，“列表”，“字典”返回值，建议配置这几个配置

********
测试示范
********

实例化 ZAP 服务后，需要配置校验策略

::

    zap = AsyncZAPService()
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )
    zap.start()


继承Worker类，用interface装饰希望被远程调用的方法，然后实例化一个Server来创建Worker的实例，这个worker实例的描述信息由server实例提供。

::

    from gaterpc.core import Context, Worker


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
    ctx = Context()
    gr = Service(name="SRkv")
    gr_worker = gr.create_worker(
        GRWorker, "inproc://gate.worker.01",
        context=ctx,
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    gr_worker.run()

当要执行 IO 密集或 CPU 密集型操作时，可以在方法内使用执行器来执行，可以使用自带的两个执行器，也可以使用自定义的；
另外，所有同步的函数都会使用默认执行器执行，默认执行器是 ThreadPoolExecutor 实例，可以修改。
如果连接地址使用的 inproc 类型，一定要和 Majordomo 使用同一个 Context。

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

实例化代理时会绑定两个地址，一个用于给后端服务连接上来，一个给前端客户端连接上来，bind 方法是绑定的给客户端访问的地址也就是前端地址。

::

    from gaterpc.core import AMajordomo, Context


    # Majordomo
    class GRMajordomo(AMajordomo):
    # 可以新增内部处理程序，用于扩展分布式应用，所有内部处理程序都必须能接收关键词参数
    # 位置参数可以自定义，也可以没有，关键词参数会被更新加入固定参数
    # kwargs 的结构是固定的
    # kwargs = {
    #    "client_id": client_id,
    #    "client_addr": client_addr,
    #    "request_id": request_id,
    #    "body": body
    # }
        @interface
        def internal_x_process(self, **kwargs):
            return stat_code

        @interface
        async def internal_y_process(self, a, b, c, **kwargs):
            return stat_code

    Settings.setup()
    ctx = Context()
    gr_majordomo = GRMajordomo(
        context=ctx,
        gate_zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        gate_zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    # 绑定后端地址，为空则使用 Settings.WORKER_ADDR
    gr_majordomo.bind_backend()
    gr_majordomo.bind_frontend("ipc:///tmp/gate-rpc/run/c1")
    # 如果需要为其他代理提供服务
    gr_majordomo.bind_gate(bind_gate)
    # 如果启用了 zap 服务
    await gr_majordomo.connect_zap(zap_addr=zipc)
    # 发起 zap 请求和等待 zap 处理结果是使用的 asyncio.Future 来处理异步等待，
    # 并且使用 LRUCache 缓存每个地址使用不同的校验策略的结果，避免频繁发起验证请求而导致增加 rpc 调用的时间
    gr_majordomo.run()
    # 如果要连接其他的代理节点，需要在本地代理启动后
    await gr_majordomo.connect_gate(connect_gate)

客户端直接连接代理地址，使用点语法调用远程方法，一般格式是 client.服务名.方法名，当直接使用 client.方法名时，会使用默认服务名调用。

::

    # Client
    Settings.setup()
    gr_cli = Client(
        zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    gr_cli.connect(check_socket_addr(frontend_addr))
    await gr_cli.GateRPC.test("a", "b", "c", time=time())
    await gr_cli.GateRPC.atest("a", "b", "c", time=time())
    async for i in await gr_cli.SRkv.test_agenerator(10):
        print(i)
    await gr_cli.test_huge_data()


客户端调用的远程方法后，会创建一个延迟回调用来删掉缓存的已经执行完毕的请求，包括超时没拿到回复的请求，
而流式回复会每次回调时都检查一次该 StreamReply 实例是否已经结束，没结束就再创建一个延迟回调后续再检查


********
注意点
********

客户端的请求和回复的异步处理是使用的 asyncio.Future ，然后使用 asyncio.wait_for 来超时等待。

::

    # 请求远程方法
    request_id = await Client._request(service_name, func_name, args, kwargs)
    response = await asyncio.wait_for(Client.replies[request_id], timeout=Client.reply_timeout)
    # 接收回复
    await Client.replies[request_id].set_result(body)

如果自定义方法的返回对象的大小无法使用 sys.getsizeof 准确获取，建议用 HugeData 包装后再返回

::

    # data 必须要是 bytes 或 bytearray，简言之能用 memoryview 包装的
    hd = HugeData(
        Settings.HUGE_DATA_END_TAG,
        Settings.HUGE_DATA_EXCEPT_TAG,
        data=data, compress_module="gzip", compress_level=9, frame_size_limit=1000
    )
    c_d = b""
    async for _d in hd.compress()
        c_d += _d
    # 或者不提供 data ，HugeData 初始化时会创建一个 Queue 的跨进程代理对象，往这个跨进程队列里传输数据即可
    hd = HugeData(
        Settings.HUGE_DATA_END_TAG,
        Settings.HUGE_DATA_EXCEPT_TAG,
        get_timeout = 10,
        compress_module="gzip", compress_level=9, frame_size_limit=1000
    )
    d = process_data()
    for i in range(0, len(d), 1000):
        _d = d[i: i + 1000]
        hd.data.put(_d)
    d_d = b""
    async for _d in hd.decompress(1000)
        d_d += _d

HugeData 的 compress 和 decompress 方法都会在进程池里执行增量压缩和增量解压缩，
返回的异步生成器每次获取的字节数大小不会超过 Settings.HUGE_DATA_SIZEOF ，
compress 方法对每一块返回的大小的限制是 HugeData 内部实现，
decompress 方法对每一块返回的大小限制则是由压缩模块来实现，会在调用解压缩器实例的 decompress 方法时传递一个 max_length 位置参数。
