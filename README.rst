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

安装
******

可以直接使用pip安装，或直接下载源码随意放置。

::

    pip install gaterpc

配置
******

在实例化 Worker、Service、AMajordomo、Client 各类之前，
需要运行 Settings.setup 函数来初始化全局配置 [#f1]_ 。

::

    # 除了直接配置 Settings的属性以外，也可以创建一个包含配置项的python文件
    # 然后在程序入口配置 Settings.USER_SETTINGS
    project/
    ├── __init__.py
    ├── package
    │   ├── __init__..py
    │   ├── settings.py
    ├── settings.py
    ├── run.py
    # 在 run.py 里
    Settings.xxxx = yyyy
    Settings.USER_SETTINGS = "package.settings or settings"
    # 当日志使用 utils.AQueueHandler类，并且listener指向的是一个使用asyncio的异步队列时，
    # Settings.setup 方法要在协程里调用，因为setup方法内会初始化日志配置。
    Settings.setup()


全局配置里有些是使用 utils.LazyAttribute 构建的描述器，在设置和获取时可能会触发校验或被修改。

例如像是 RUN_PATH 和 LOG_PATH 这类路径的配置，如果只配置了BASE_PATH，
那么在调用这类属性时，会直接拼接在 BASE_PATH 下，并创建出真实目录，
而在配置该类属性时，则会触发类型检查，保证配置的是 pathlib.Path 类的实例。

自定义配置也可以使用 utils.LazyAttribute 包装成描述器，以便不用立即配置，而是在调用时才配置。

LazyAttribute 初始化时接受三个可选参数：

- raw: 原始值，默认是 empty
- render: 一个 Callable 对象，必须接受两个位置参数，在实例调用该描述器所表示的属性时，会传递调用实例和原始值，并将返回值返回给调用实例
- process: 一个 Callable 对象，必须接受两个位置参数，在实例设置该描述器所表示的属性时，会传递调用实例和原始值，并将返回值保存为该描述器的原始值


::

    import os
    from pathlib import Path
    from gaterpc.utils import LazyAttribute, empty, TypeValidator, ensure_mkdir


    BASE_PATH = Path("xxx")
    NAME = os.getenv("NAME", "MyGATE")
    # 使用实例已经有的值来配置
    ZAP_PLAIN_DEFAULT_USER = LazyAttribute(
        render=lambda instance, p: instance.NAME if p is empty else p
    )
    # 在实例设置属性时对值进行校验
    A_PATH = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.BASE_PATH.joinpath("a")
        ) if p is empty else ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    # 通过在实例设置属性时抛出异常来阻止设置该属性
    B_PATH = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.A_PATH.joinpath("b")
        ),
        process=lambda instance, p: (_ for _ in ()).throw(AttributeError)
    )


以下是可能修改的一些主要配置的解释

- HEARTBEAT: int = 全局的默认的心跳间隔，单位是毫秒
- TIMEOUT: float = 全局的默认超时时间，单位是秒
- MESSAGE_MAX: int = Worker 和 Client 实例里等待处理的消息最大数量
- HUGE_DATA_SIZEOF: int = 每次传输的结果值的最大大小，超过该值的将会被压缩并分片传输
- HUGE_DATA_COMPRESS_MODULE: str = 使用的压缩模块的名称 [#f1]_
- SERVICE_DEFAULT_NAME: str = 默认的服务名，当在实例化 Service 时如果不提供 name 参数则会以这个为服务名
- WORKER_ADDR: str = Majordomo 用于绑定给 Worker 连接的地址，一般是 inproc 类型，当使用 inproc 地址时，需要和 Majordomo 使用同一个 Context
- MDP_INTERNAL_SERVICE_PREFIX: bytes = MDP 内部服务的前缀
- MDP_HEARTBEAT_INTERVAL: int = 服务端和客户端相对于中间代理的心跳间隔时间，单位是毫秒，默认是使用上面的HEARTBEAT
- MDP_HEARTBEAT_LIVENESS: int = 心跳活跃度，用于判定掉线的丢失心跳次数，即当超过该次数*心跳时间没有收到心跳则认为已经掉线，默认3次
- REPLY_TIMEOUT: float = 客户端调用远程方法时，等待回复的超时时间，应设置的远远大于心跳时间，默认是一分钟
- STREAM_REPLY_MAXSIZE: int = 流式数据使用的缓存队列的最大长度（使用的 asyncio.Queue）
- REPLY_TIMEOUT: float = 获取回复的超时时间，也是流式传输的每一个子回复的超时时间，单位是秒，默认使用的全局的TIMEOUT
- ZAP_PLAIN_DEFAULT_USER: str = ZAP 的 PLAIN 机制的默认用户名
- ZAP_PLAIN_DEFAULT_PASSWORD: str = ZAP 的 PLAIN 机制的默认密码
- ZAP_ADDR: str = ZAP 服务绑定的地址，如果是和代理服务一起使用，最好使用 ipc 类型，且不要和代理使用同一个 Context
- ZAP_REPLY_TIMEOUT: float = 等待 ZAP 服务的回复的超时时间，单位是秒，远比普通的REPLY_TIMEOUT短，因为zap服务处理每一个zap请求必须很快
- GATE_CLUSTER_NAME: str = gate集群的集群名
- GATE_CLUSTER_DESCRIPTION: str = gate集群的描述
- MEMBER: str = gate集群的成员版本

特殊返回值的序列化通过 MessagePack 的全局实例（gaterpc.utils.message_pack）来定制 [#f2]_ 。

::

    from gaterpc.utils import message_pack
    message_pack.prepare_pack = 在使用 msgpack.packb 时，传递给 default 参数的可执行对象
    message_pack.unpack_object_hook = 在使用 msgpack.unpackb 时，传递给 object_hook 的可执行对象
    message_pack.unpack_object_pairs_hook = 在使用 msgpack.unpackb 时，传递给 object_pairs_hook 的可执行对象
    message_pack.unpack_object_list_hook = 在使用 msgpack.unpackb 时，传递给 list_hook 的可执行对象

.. rubric:: Footnotes

.. [#f1] Settings.HUGE_DATA_COMPRESS_MODULE 除了内置的 gzip，bz2，lzma，还可以使用外部模块，只要模块提供 compressor 和 decompressor 方法即可，
   compressor 需要返回一个带有 compress 方法的增量压缩器对象，decompressor 需要返回一个带有 decompress 的增量解压缩器对象
.. [#f2] 单一返回值和生成器的元素返回值，以及巨型返回值都会使用 utils.msg_pack 和 utils.msg_unpack 来序列化和反序列化，
   这两个方法内部是使用的 utils.MessagePack 的全局实例，如果不能返回常规的“字符串”，“列表”，“字典”的返回值，建议配置这几个配置。

测试示范
********

实例化 ZAP 服务后，需要配置校验策略。

::

    zap = AsyncZAPService()
    zap.configure_plain(
        Settings.ZAP_DEFAULT_DOMAIN,
        {
            Settings.ZAP_PLAIN_DEFAULT_USER: Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        }
    )
    zap.start()

继承Worker类，用interface装饰希望被远程调用的方法，
然后实例化一个Server来创建Worker的实例，这个worker实例的描述信息由server实例提供。

::

    from gaterpc.core import Context, Worker
    from gaterpc.utils import interface

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

        @interface("process"):
            cpu_bound()

        @interface("thread")
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

    async def test():
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

当要执行 IO 密集或 CPU 密集型操作时，可以通过interface装饰器指定是否使用在执行器里运行，
也可以不通过interface指定，而是在方法内使用run_in_executor，也可以使用自定义的。

另外，所有同步的函数都会使用默认执行器执行，默认执行器是 ThreadPoolExecutor 实例，可以修改。

如果连接地址使用的 inproc 类型，一定要和 Majordomo 使用同一个 Context。

::

    @interface("thread")
    async def test_io():
        return result

    @interface
    async def test_io():
        result = await self.run_in_executor(self.thread_executor, func, *args, **kwargs)
        return result

    @interface
    async def test_cpu():
        # 如果需要和 CPU 密集型执行器里的方法交换数据，
        # 可以使用 utils 模块内定义的全局代理管理器 SyncManager 来创建代理对象使用。
        queue = SyncManager.Queue()
        result = await self.run_in_executor(self.process_executor, func, queue, *args, **kwargs)
        return result

实例化代理时要绑定两个地址，一个用于给后端服务连接上来，一个给前端客户端连接上来。

也可以只绑定后端地址，将代理实例作为前端使用，适合不长期自动运行的任务（参见test/testMajordomo.py）。
还可以只绑定前端地址，将代理实例作为后端使用，适合简单的rpc调用。

::

    from gaterpc.core import AMajordomo, Context
    from gaterpc.utils import interface


    Settings.setup()
    ctx = Context()
    majordomo = Majordomo(
        context=ctx,
        gate_zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        gate_zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )
    # 绑定后端地址，为空则使用 Settings.WORKER_ADDR
    majordomo.bind_backend()
    majordomo.bind_frontend("ipc:///tmp/gate-rpc/run/c1")
    # 如果启用了 zap 服务
    await majordomo.connect_zap(zap_addr=zipc)
    # 发起 zap 请求和等待 zap 处理结果是使用的 asyncio.Future 来处理异步等待，
    # 并且使用 LRUCache 缓存每个地址使用不同的校验策略的结果，避免频繁发起验证请求而导致增加 rpc 调用的时间
    majordomo.run()

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
而流式回复会每次回调时都检查一次该 StreamReply 实例是否已经结束，没结束就再创建一个延迟回调后续再检查。

更详细的测试用例可以看看test目录下的测试脚本

Gate cluster
************

当布置多代理集群时，用 bind_gate 绑定集群节点地址。

在 Gate 集群内各个节点可以转发当前节点的前端请求到其他节点，
也可以请求其他节点的内部方法（比如分布式算法的集群节点选举）,
内部方法必须返回一个由状态码和结果组成的元组。

内部服务创建：

::

    from gaterpc.utils import interface
    from gaterpc.core import AMajordomo

    class Gate(AMajordomo):
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
        def internal_service(self, *args, **kwargs):
            status_code = b"200" # response code
            result = Any
            return status_code, result

    Settings.setup()
    ctx = Context()
    gate = Gate(
        context=ctx,
        gate_zap_mechanism=Settings.ZAP_MECHANISM_PLAIN,
        gate_zap_credentials=(
            Settings.ZAP_PLAIN_DEFAULT_USER,
            Settings.ZAP_PLAIN_DEFAULT_PASSWORD
        )
    )

    # 为其他代理提供服务
    gate.bind_gate(bind_gate)
    # 如果启用了 zap 服务
    await gate.connect_zap(zap_addr=zipc)
    # 要连接其他的代理节点，需要在本地代理启动后
    await gate.connect_gate(connect_gate)

笔记
******

客户端的请求和回复的异步处理是使用的 asyncio.Future ，然后使用 asyncio.wait_for 来超时等待。

::

    # 请求远程方法
    request_id = await Client._request(service_name, func_name, args, kwargs)
    response = await asyncio.wait_for(Client.replies[request_id], timeout=Client.reply_timeout)
    # 接收回复
    await Client.replies[request_id].set_result(body)

如果自定义方法的不返回对象的大小无法使用 sys.getsizeof 准确获取，建议用 HugeData 包装后再返回

::

    # data 必须要是 bytes ，会通过 SharedMemory 或 os.pipe 来传递给压缩器或解压缩器
    hd = HugeData(
        Settings.HUGE_DATA_END_TAG,
        Settings.HUGE_DATA_EXCEPT_TAG,
        data=data, compress_module="gzip", compress_level=9, blksize=1000
    )
    c_d = b""
    async for _d in hd.compress():
        c_d += _d
    # 或者不提供 data ，HugeData 初始化时会创建一个 os.pipe 的管道，然后通过 add_data 追加需要处理的数据
    hd = HugeData(
        Settings.HUGE_DATA_END_TAG,
        Settings.HUGE_DATA_EXCEPT_TAG,
        compress_module="gzip", compress_level=9, blksize=1000
    )
    d = process_data()
    # 可以整个直接丢进去
    hd.add_data(d)
    # 或者分块传递
    for i in range(0, len(d), 1000):
        _d = d[i: i + 1000]
        hd.add_data(_d)
    # 数据添加完毕后，务必调用一下flush方法
    hd.flush()
    d_d = b""
    # 传递未处理数据和接收已处理数据可以异步执行
    async for _d in hd.decompress(1000):
        d_d += _d

HugeData 的 compress 和 decompress 方法都会在进程池里执行增量压缩和增量解压缩，
返回的异步生成器每次获取的字节数大小可以通过初始化 HugeData 时传递 blksize 来限制，
compress 方法对每一块返回的大小的限制是 HugeData 内部实现，
decompress 方法对每一块返回的大小限制则是由压缩模块来实现，
会在调用解压缩器实例的 decompress 方法时传递一个 max_length 位置参数。


在使用由"gaterpc.utils.AQueueHandler"做为处理器的日志处理器时，
要避免跨越线程和跨事件循环实例来记录日志，
在将StreamHandler作为AQueueHandler的handler_class参数时会就遇到跨事件循环调用的错误
