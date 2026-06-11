# Runtime 模块化审计

## 当前拆分

- `runtime/config_types.hpp`、`runtime/config_resolution.hpp`：public 结构化 runtime 配置类型与解析校验入口，`runtime_config.hpp` 只保留 umbrella include。
- `runtime/runtime.hpp`、`runtime/detail/runtime_lifecycle.hpp`：runtime 实例生命周期、线程启动停止、task/service/reactor 注册入口。
- `runtime/detail/executor.hpp`、`runtime/detail/executor_impl.hpp`：executor 对象布局、intrusive MPSC inbox、task 执行循环、service task 轮询。
- `reactor/reactor.hpp`、`reactor/fd_event_source.hpp`、`reactor/detail/epoll_reactor.hpp`、`reactor/detail/kqueue_reactor.hpp`、`reactor/detail/select_reactor.hpp`：统一 reactor 抽象、fd event source 和平台 backend；`af/reactor.hpp` 是 reactor 模块 umbrella。
- `memory/cache_line.hpp`、`memory/object_pool*.hpp`、`memory/contiguous_object_storage.hpp`：cache-line 对齐基础设施、对象池与连续对象存储；热原子包装不再放在 runtime common state 中。
- `timer/timer_backend.hpp`、`timer/timer_entry.hpp`、`timer/timer_heap.hpp`、`timer/hierarchical_timer_wheel.hpp`：executor 本地 timer backend 聚合入口、timer entry、min-heap backend 与默认分层时间轮；timer 数据结构已从 runtime detail 目录移入 timer 模块并按职责拆分。
- `runtime/task.hpp`、`runtime/task_impl.hpp`：runtime task 创建、调度、task id 和对象池生命周期。
- `log/config.hpp`、`log/logger.hpp`、`log/log_backend.hpp`、`log/file_backend.hpp`、`log/udp_backend.hpp`、`log/tcp_backend.hpp`、`log/log_record.hpp`：日志公开配置、logger、record 和后端入口。
- `log/detail/`：async logger、日志队列、record pool、runtime 绑定后端和 Abseil sink；日志内部实现已归属到 log 模块下。
- `detail/runtime/`：只保留 `atomic_wait`、`cpu_relax`、`runtime_common_state`、`runtime_service_task`、`timed_atomic_wait` 等跨模块底层组件。

## 本次收敛结果

旧 `AsyncRuntime<Traits>` 静态模板 runtime、旧 executor backend、旧 task registry、依赖 `Runtime::Thread` 的模板 helper 以及 runtime parallel group 的 CamelCase 兼容 alias 已移除。当前公开 runtime 只保留实例化 `af::runtime` 路径，线程布局由结构化 `runtime_config` 解析得到。

旧模板 TCP/UDP server/client 及其 detail 实现已移除，`af/net.hpp` 只导出 runtime-native TCP/UDP/Unix API。TCP connection handle 的跨线程操作通过 runtime task/post 调度到 owner reactor thread，不再依赖旧 control thread 状态。

`af/async_flow.hpp` 已从默认 umbrella 中移除旧 `af/io.hpp` facade，并改为导出 `af/net.hpp`。旧 task 级 async IO facade 已物理删除：`include/af/io*.hpp` 与 `include/af/detail/io/` 不再作为公开或兼容入口存在。新网络代码统一走 runtime-native reactor/net API。

旧 `supports_native_io_wait` / `AF_DETAIL_HAS_NATIVE_IO_WAIT` 命名已移除，平台能力改为 `supports_reactor_backend` 与具体 `supports_epoll`、`supports_kqueue`、`supports_select`，避免旧 async IO facade 语义继续污染 reactor 模型。

共享网络 endpoint 类型已从旧 `net/tcp_endpoint.hpp` 文件名迁移到 `net/endpoint.hpp`，避免 TCP/UDP/Unix endpoint 共用一个 TCP 专属入口名；public header 测试会阻止旧头重新出现。

网络内部 socket address helper 已从全局 `detail/net/socket_address.hpp` 移到 `net/detail/socket_address.hpp`，使网络 detail 归属到 net 模块下。

网络公开入口开始按协议拆分：`af/net.hpp` 现在通过 `net/tcp/tcp_server.hpp`、`net/tcp/tcp_client.hpp`、`net/tcp/tcp_connection.hpp`、`net/tcp/tcp_listener.hpp`、`net/udp/udp_socket.hpp` 和 `net/unix/*` 聚合 TCP/UDP/Unix API，不再直接包含旧平铺实现头。旧平铺头暂时保留以降低迁移风险。

queue 基础结构已从 `include/af/detail/queue/` 迁移到 `include/af/queue/`，包含 intrusive MPSC、bounded MPSC/MPMC、公共 ring 序号工具和 backoff。`tests/public_header_tests.cpp` 会阻止旧 `detail/queue` 头文件重新出现，并确认新路径可包含。

日志内部实现已从全局 `include/af/detail/log/` 迁移到 `include/af/log/detail/`，公开入口继续保持 `af/log.hpp`；public header 测试会阻止旧日志 detail 路径重新出现。

`af/log.hpp` 和 runtime 配置现在通过 `include/af/log/*.hpp` public forwarding headers 依赖日志模块，不再直接包含 `af/log/detail/*`。这先把 public/detail include 边界固定住，后续可继续把 logger 与后端实现从转发头拆成更细的公开声明和 detail 实现。

对外命名继续向 lower_case 迁移：batch/crud/parallel utility、compile-time `thread_layout`、task 状态枚举、signal、buffer 以及 log 配置/句柄/后端相关主类型已迁移为 lower_case。public `af::net`、utility、log、task 状态枚举、对象池/log/基础设施 detail 与 compile-time `thread_layout` 的 CamelCase 类型 alias 已删除，并通过 public header 源码扫描测试防回归。examples 中的 runtime task、stream tag 和业务 batch 类型也已迁移为 lower_snake_case；剩余 CamelCase 主要在测试 fixture 内部，用于覆盖旧构造路径、异常路径或平台行为。

通用 service task 由 runtime executor 按预算轮询执行；service 自身负责 pending 状态和内部队列，跨线程 producer 通过 `wake_service_tasks()` 唤醒 executor。runtime async logger 现在就是一个 service task，消费热路径不进入 task 状态机；推荐手工入口是 `start_runtime_logging()`，runtime 配置了日志后会在 `runtime::start()` 中自动启动并在 `runtime::stop()` 中 drain/flush。

## 模块边界

- runtime 调度和 IO 后端分离。
- runtime reactor 和网络连接生命周期分离；默认公共入口只暴露 runtime-native reactor/net。
- 网络连接生命周期不依赖普通 task pending/resume 热路径。
- 日志 consumer 通过通用 service task 绑定 runtime 线程，不再使用长期 runtime task 驱动消费；注册/注销仍通过短 control task 在 owner executor 上完成。
- `make_task<T>()` 返回非空原始 task 指针，`try_make_task<T>()` 在可恢复创建失败时返回 `nullptr`；推荐写法是 `auto* task = make_task<T>(runtime); task->do_it(...)` 两段式启动，examples 已统一到该形态。首次 `do_it`/调度会消费 created 生命周期引用，旧 RAII task handle 兼容层已移除。
- service task 注册/注销要求在 owner runtime 线程执行，列表不加锁；跨线程唤醒不修改列表。
- 对象池与日志 record pool 已具备 local cache、批量回收、无锁 free stack/slab free list 和 cache-line slot 对齐；后续性能复核应优先用 benchmark/perf 证明热点，再决定是否引入更复杂的 per-thread/per-shard 池化策略。

## 后续拆分建议

- 网络 reactor 继续按 poller、channel、tcp server、tcp connection、udp socket、unix socket 拆分。
- 示例和测试保持按功能命名，避免历史后端名进入文件名。
