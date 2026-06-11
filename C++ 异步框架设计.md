# AsyncFlow C++ 异步框架目标架构

本文档是 AsyncFlow 下一代架构的中文总览。完整的配置、线程、task、对象池、timer、reactor、network、logger、生命周期和迁移计划见 [docs/next_runtime_architecture.md](docs/next_runtime_architecture.md)。

## 核心定位

AsyncFlow 不是通用线程池，而是固定 executor/event-loop 异步框架。业务先声明线程布局，再把任务、网络服务、日志消费者等组件绑定到明确的框架线程上运行。

目标形态：

- C++17。
- 线程类型只保留 `thread_kind::io` 和 `thread_kind::cpu`。
- IO 后端统一抽象为 reactor，对上屏蔽 epoll/kqueue/select。
- 不再保留 io_uring 路径和兼容桩。
- 每个 executor 一个 intrusive unbounded MPSC task inbox。
- 不使用 local queue，不使用 SPSC queue。
- task、timer、log record、net buffer 使用高性能 slab/local-cache 对象池。
- 日志消费者绑定 runtime 线程，以 service task 方式运行，不创建独立日志线程。
- 网络 fd 操作只在 owner reactor 线程执行，跨线程操作通过 task 显式调度。

## Runtime

```text
af::runtime
  executor[]
    task inbox
    timer backend
    reactor, only io executor
    service tasks
  task pool
  timer pool
  log record pool
  logger
```

CPU executor 负责 task、timer、service task，并在空闲时 futex/atomic wait。IO executor 在同一循环里处理 task、timer、service task 和 reactor readiness，避免 IO 线程阻塞时饿死调度到本线程的任务。

## 配置

配置使用普通 `struct`，不使用 builder 链式构造：

```cpp
af::runtime_config cfg;

cfg.threads = {
    af::io_threads("io", 4),
    af::cpu_threads("logic", 8),
};

cfg.logger = af::log_config::ordered();
cfg.logger.consumer_thread = af::thread_selector::cpu(0);
cfg.logger.backends = {
    af::file_log_backend_config{.path = "server.log"},
};

af::runtime rt(cfg);
```

命名工厂函数只生成配置值，例如 `io_threads()`、`cpu_threads()`、`log_config::ordered()`、`tcp_endpoint::any()`。

## Task

任务只能通过 runtime 创建：

```cpp
auto* task = af::make_task<login_task>(rt);
task->do_it(conn, req);
```

`make_task<T>()` 默认返回非空指针。对象池会持续扩展 slab，除非系统 OOM。需要可恢复失败时使用 `try_make_task<T>()`。

调度 API 统一使用 `schedule` 语义：

```cpp
schedule_to(thread_ref target);
schedule_after(duration delay);
schedule_after(thread_ref target, duration delay);
schedule_at(time_point time);
schedule_at(thread_ref target, time_point time);
reschedule();
done();
cancel();
```

用户只需要表达任务下一次在哪个线程、哪个时间继续执行，不需要区分首次 `do_it()` 和后续 `run()` 的内部状态差异。

## Reactor 与网络

每个 IO executor 一个 reactor：

```text
io executor 0 -> reactor 0 -> epoll/kqueue/select
io executor 1 -> reactor 1 -> epoll/kqueue/select
```

网络层建立在 reactor 上：

```text
tcp_server
tcp_listener
tcp_connection
tcp_client
udp_socket
unix_stream_server
unix_stream_client
unix_datagram_socket
```

所有真实 fd 操作都在 owner IO 线程执行。外部线程或 CPU task 要发送、关闭、增加 listener、停止 server 时，先 `schedule_to(owner_thread)`。

TCP/UDP/Unix 层只提供高性能 stream/datagram 能力，不内置 packet codec 或 dispatcher。包长度、包 id、protobuf/json 解析和业务 handler 分发由用户按业务组合；protobuf 只用于 tcp login server 示例。

## Logger

日志消费者是 runtime service task：

```text
LOG
 -> level check
 -> Abseil format
 -> acquire log_record
 -> bounded MPSC ingress
 -> wake consumer service task
 -> batch drain
 -> backend write
 -> recycle log_record
```

默认 ordered 策略，支持 relaxed 策略作为显式配置。用户日志入口使用 `AF_LOG` / `AF_LOG_IF`，日志等级过滤发生在 Abseil stream 格式化之前，不匹配等级时不会格式化用户消息。日志队列 bounded，默认满时 drop newest 并记录 dropped counter；log record pool 不 bounded。

## 性能边界

- task inbox unbounded，非首次调度不处理“队列满”分支。
- 对象池快路径走 local cache，跨线程释放批量回收。
- 热路径结构按 cache line 对齐，避免 false sharing。
- reactor 默认 LT 模式，只在 fd 注册、interest 变化、取消和关闭时修改内核状态。
- TCP/UDP 按预算 drain，避免单连接或单 socket 饿死同线程其他工作。
- hot path 优先数组、slot table、generation 和 `buffer_view`；冷控制面可使用 `absl::flat_hash_map`。

## 构建与验证

依赖由 conan 管理：

- `abseil/20260107.1`
- `gtest/1.17.0`
- `benchmark/1.9.5`
- `protobuf/7.35.0`

验证重点：

- GCC/Clang 构建。
- 全量 ctest。
- task、timer、MPSC、对象池、logger、reactor、TCP/UDP/Unix stress。
- benchmark 和 perf 分析 cache misses、branch misses、cycles、syscalls、context switches。
