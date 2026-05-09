# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 规范，
版本号采用 [SemVer](https://semver.org/lang/zh-CN/)。

## [0.3.0] - 2026-05-09

经过两轮全面审查（性能 + 安全），本次发布修复了大量长期存在的正确性、安全和性能问题，
并新增端到端测试。涉及 `gaterpc/core.py`、`gaterpc/utils.py`、`gaterpc/global_settings.py`。

### Security

- **ZAP 缓存键加入 `domain` 与 `channel`**（`core.py: request_zap`）。原实现仅以 `(address, mechanism, *credentials)` 作键，
  导致同一凭据可在不同域 / 不同链路（frontend / backend / gate）间相互复用并绕过 ZAP 服务的角色校验。
  新键为 `(channel, zap_domain, address, mechanism, *credentials)`，三类链路完全隔离。
- **`request_zap` 增加 `channel` 参数**，frontend / backend / gate 三路调用点显式区分。
- **msgpack 反序列化加上限**（`utils.MessagePack.loads`）。新增 `max_str_len`、`max_bin_len`、
  `max_array_len`、`max_map_len`，默认 32 MB / 1M 元素；`raw=False` 强制 UTF-8 解码字符串。
- **HugeData 解压加防 zip-bomb**（`utils.HugeData.decompress`）。新增 `max_decompressed_size`
  参数（默认 256 MB），累计解压字节超限抛 `HugeDataException`。
- **`Worker.process_request` 输入校验**：`args` 必须为 list/tuple、`kwargs` 必须为 dict 且键为 str
  且不允许 `__` 前缀；防止远端通过特制 kwargs 注入 `__client_id` / `__request_id` 之类内部字段。
- **`Gate.internal_interface` kwargs 改用 `__client_id` / `__request_id`**，与 `AMajordomo`
  保持一致，消除被 caller 在 kwargs 中预先填充而被 `update` 后写覆盖的攻击面。
- **`update_service` 用类型/边界判断替代 `assert`**，避免 `python -O` 下被去掉；
  `num` 取值限定在 `[0, MESSAGE_MAX]`。
- **`process_replies_from_zap` 防御未知/已 done 的 `reply_key`**，避免攻击者构造未知 reply 引起 KeyError 噪音。
- **`zap_replies` 在 `try / finally` 里统一 `pop(reply_key, None)`**，超时分支不再泄漏 future。
- **多播 NOTICE 报文的端口加 `1 ≤ port ≤ 65535` 校验**，丢弃带符号 int 解析得到的非法端口。
- **`STREAM_REPLY_MAXSIZE` 默认 `0` → `1024`**，避免恶意 worker 把客户端 `asyncio.Queue` 灌爆。

### Fixed (Correctness)

- **`set_sock` / `set_ctx` 不再原地修改全局 `Settings.ZMQ_SOCK` / `ZMQ_CONTEXT`**。
  原实现 `_options = Settings.ZMQ_SOCK; _options.update(options)` 会把当前 socket 的
  `IDENTITY` 等参数累积到全局，造成后续 socket 串台。
- **`StreamReply` 删除已被 Python 3.10 移除的 `loop=` 参数**（`asyncio.Queue` / `asyncio.wait_for`）；
  原代码在 3.10+ 直接 `TypeError`。
- **`StreamReply.__anext__`**：
  - 删除结束时多余的 `await asyncio.sleep(0.1)` 延迟；
  - 从 `get_nowait` 改为 `await self.replies.get()`，避免在数据尚未到达时立即抛 `QueueEmpty`；
  - `_exit` 时 raise 改为 `StopAsyncIteration`（原先抛的 `GeneratorExit` 不符合异步迭代协议）。
- **`Client` / `SimpleClient` 远程方法代理改为不可变 `_RemoteMethodProxy`**。
  原实现把 `_remote_service` / `_remote_func` 写在 `Client` 实例上，并发调用
  `client.SvcA.foo()` 与 `client.SvcB.bar()` 会互相覆盖路径。新代理每次属性访问返回新实例，
  E2E 并发 20 路独立请求结果完全正确。
- **`Worker.process_request` 在 dispatch 前对同步 `Generator` 也做 `generator_to_agenerator` 转换**，
  修复同步生成器返回值会进入 `msg_pack(AsyncGenerator)` 错误路径的 pre-existing bug。
- **`AMajordomo.process_replies_from_zap`**：未注册的 `reply_key` 直接 return，已 done 的 future 不再二次 set。
- **`process_gate_reply` 同步 `pop(request_id)`**，避免 `request_map` 长期泄漏。
- **`internal_requests` 在 reply 路径同步 `pop`**，避免长跑 broker 字典无限增长。
- **`SyncManager` 改为 `_LazySyncManager` 懒加载**：
  模块加载不再触发 `multiprocessing.Manager()` fork 出 manager 子进程，
  显著减小 import 副作用，并避免在受限环境（forkserver / 容器）里 import 即失败。

### Performance

- **`LazyAttribute` 描述器加 per-instance 缓存**（`utils.LazyAttribute`）。
  对带 `mkdir` 副作用的属性（`LOG_PATH` / `RUN_PATH` 等）提供约 **40× 加速**；
  对常量类（`MDP_CLIENT` / `MDP_WORKER` / `GATE_MEMBER`）约 **1.15×**。
  `__set__` 与 `Settings.configure` 触发缓存失效。
- **`Client._request` 解压路径**：
  不再每次新建 `ProcessPoolExecutor`；
  `result = b""; result += data` 的 O(n²) 累加改为 `chunks = []; b"".join(chunks)`。
- **`HugeData._write_data1`**：写偏移超过 1 MB 阈值后周期性 `del buffer[:offset]; offset = 0`，
  避免长流场景 `bytearray` 无限增长。
- **`HugeData.add_data` / `_write_data1`** 之前已修，本版新增数据流场景下的 buffer 截断。
- **`GateClusterService.ready_workers` 维护字段化**：
  `get_workers()` 从每次 dict 推导 + filter 改为 O(1) 直接返回，hot path 调用十多处。
- **`AMajordomo.send_notice_command`** 多个 gate 串行 await → `asyncio.gather(...)` 并行下发。
- **`build_zap_frames` / `build_gate_zap_frames` / `_build_zap_frames`** 全部改 sync def + 一次性预编码，
  send 热路径不再每条消息构建临时 coroutine + 重新 encode 凭据。
- **`_remote_services: list` → `set`**，命中检测 O(n) → O(1)。
- **`Client._clear_reply` 自递归改 `loop.call_later`**，原实现对未结束的 StreamReply 立即重新 `create_task`，
  导致 CPU 100% 空转。
- **后端 `READY` 命令的噪音 warning 抑制**：仅当非 READY 命令找不到 worker 时打 warning，
  并改用 `%s` 形式让 logging 内部决定是否构造字符串。
- **HugeData ProcessPoolExecutor 复用**（前序版本已修）：`get_process_executor` 类级共享 + `BrokenProcessPool` 自愈。
- **KVLRUCache tracking 改为 `debug=True` 时才启用**（前序版本已修）：生产模式 `__getitem__` 零额外开销。
- **QueueListener / AQueueListener 监听任务用 `weakref.ref(self)` 闭包**（前序版本已修）：
  解除 self 强引用，配合 `weakref.finalize` 让 GC 真正能回收实例。

### Added

- **`test/testE2E.py`**：单进程 Client → AMajordomo → Worker 端到端回归测试，
  覆盖异步方法、线程同步方法、同步生成器、异步生成器、异常透传、并发独立代理链、
  sequential 100 次顺序请求、不存在方法等 8 个用例。
- **`HugeData.max_decompressed_size`** 配置项 + `DEFAULT_MAX_DECOMPRESSED_SIZE` 类属性。
- **`MessagePack.max_str_len` / `max_bin_len` / `max_array_len` / `max_map_len`** 实例可配置上限。

### Changed

- 版本号从 `0.2.11` → `0.3.0`（语义化：含正确性破坏性修复 + 行为变更）。
- `STREAM_REPLY_MAXSIZE` 默认值 `0` → `1024`（接口兼容，行为更安全）。
- `Client._remote_services` 类型 `list[str]` → `set[str]`。

### Removed

- `HugeData.__del__`：与 `weakref.finalize` 重复，删除以避免双路径竞态。
- `Client._clear_replies_tasks` 字段：`_clear_reply` 改 `call_later` 后无用。
- `QueueListener._monitor` / `AQueueListener._monitor` 实例方法：
  改为 `start()` 内的闭包，断开线程 / 任务对 self 的强引用。

### Notes

- 端到端测试通过条件：项目 venv 在 `.venv/`，
  使用 `inproc://` 作为 backend、`ipc://` 作为 frontend，避免依赖 `ProcessPoolExecutor`。
- 仍待后续版本处理（已记录但本次未改）：
  - 多播 NOTICE 报文加 HMAC + 单调 nonce（S3）。
  - Gate 集群 CURVE 改为每节点独立 keypair + ZAP `configure_curve` 维护可信公钥目录。
  - `process_request` 对 client_id / request_id 帧的长度上限。
  - ZAP 调试日志中可能落盘的明文密码（DEBUG > 1 时）。

[0.3.0]: https://github.com/ShiFan/gate-rpc/releases/tag/v0.3.0
