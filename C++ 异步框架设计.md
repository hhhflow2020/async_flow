# AsyncFlow C++ 异步框架目标架构

本文档记录 AsyncFlow 下一阶段的目标架构。它不是对当前代码逐行行为的说明，而是后续重构和实现时应对齐的设计蓝图。更详细的分层、配置、生命周期和 API 草案见 [docs/next_runtime_architecture.md](docs/next_runtime_architecture.md)。

## 设计目标

- C++17 标准，允许使用 C++17 及以上编译器编译。
- 配置使用普通 `struct` 和少量命名工厂函数，不使用 builder 链式构造。
- 类、函数、变量统一使用 `lower_case`，成员变量使用尾部 `_`。
- 线程类型只保留 `io` 和 `cpu`，epoll/kqueue/select 归入 reactor 后端细节。
- 每个 executor 一个 intrusive unbounded MPSC task inbox，不再使用 local queue 和 SPSC queue。
- 对象池是高性能分配器，不是容量限制器；持续扩展 slab，直到系统 OOM。
- IO 使用统一 reactor 抽象，对上屏蔽 epoll/kqueue/select。
- 日志消费者绑定 runtime 线程，以 service task 方式运行，不创建独立日志线程。
- TCP/UDP/Unix socket 对象归属明确的 reactor 线程，跨线程操作通过 task 显式 `schedule_to(owner_thread)`。

## 总体结构

```text
af::runtime
  runtime_config
  executor[]
    intrusive_mpsc_task_queue
    timer_wheel
    reactor, only io executor
    service_task list
  task_pool
  timer_pool
  log_record_pool
  logger
```

`runtime` 负责线程、任务、定时器、reactor 和 service task 的调度能力。网络、日志、metrics、trace 等组件挂在这些能力上运行，不把组件细节写死进 executor 主循环。

## 配置模型

配置是普通数据对象，用户可以直接读写字段：

```cpp
af::runtime_config cfg;

cfg.threads = {
    af::io_threads("io", 4),
    af::cpu_threads("cpu", 8),
};

cfg.scheduler.task_drain_budget = 256;
cfg.logger = af::log_config::ordered();
cfg.logger.consumer_thread = af::thread_selector::cpu(0);
cfg.logger.backends = {
    af::file_log_backend_config{"server.log"},
};

af::runtime rt(cfg);
```

允许保留少量命名工厂函数，例如 `af::io_threads("io", 4)`、`af::log_config::ordered()`、`af::tcp_endpoint::any(8080)`。这些函数只负责生成配置值，不隐藏运行逻辑。

## 线程与调度

每个 executor 拥有一个入口队列：

```text
intrusive_unbounded_mpsc_queue<task>
```

任务调度 API 统一使用 `schedule` 语义：

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

用户不需要理解 do-it 阶段和 run 阶段的内部差异。用户只需要表达任务下一次应该在哪个线程、哪个时间继续执行。

## Executor 循环

CPU executor：

```text
drain task queue with budget
run due timers with budget
run service tasks with budget
empty -> futex park
```

IO executor：

```text
drain task queue with budget
run due timers with budget
run service tasks with budget
reactor.poll(timeout)
handle io events
empty -> reactor/futex park
```

这种结构让 IO 线程既能处理 fd 事件，也能处理调度到本线程的 task，同时避免某类工作长时间占满线程。

## Task 与对象池

任务只能通过框架创建：

```cpp
auto* task = af::make_task<login_task>(rt);
task->do_it(conn, req);
```

`make_task` 默认保证返回有效指针。对象池路径：

```text
local cache
 -> remote free queue drain
 -> slab refill
 -> system allocate slab
 -> OOM policy
```

对象池按类型或 size class 管理。task、log record、timer node、net buffer 不混用一个池。跨线程释放进入 owner 的 remote free MPSC，由 owner 批量回收，减少 cache line 来回失效。

每个 task 创建时分配 `task_id_`，用于日志、trace 和问题定位。

## Timer

每个 executor 自己拥有 timer wheel。跨线程创建定时器，本质是把 timer 注册操作调度到目标 executor，由目标 executor 管理生命周期。IO executor 的 reactor timeout 由最近 timer 决定，不需要额外定时器线程。

## Reactor 与网络

每个 IO executor 一个 reactor：

```text
io executor 0 -> reactor 0 -> epoll/kqueue/select
io executor 1 -> reactor 1 -> epoll/kqueue/select
```

reactor 提供统一接口：

```cpp
reactor.add(fd_event_source*);
reactor.mod(fd_event_source*);
reactor.del(fd_event_source*);
reactor.poll(timeout);
reactor.wake();
```

网络层建立在 reactor 上：

```text
tcp_server
tcp_listener
tcp_connection
udp_socket
unix_listener
unix_connection
```

TCP/UDP 对象都是 reactor-affine。真实 fd 操作只能在 owner IO 线程执行。业务任务持有 `tcp_connection_handle`，需要发送或关闭时显式切回连接 owner 线程。

```cpp
schedule_to(conn.owner_thread());
conn.send(buffer);
```

TCP 层只提供字节流事件；packet id、长度字段、protobuf/json 解析和分发由业务层组合。

## 日志

日志由 runtime 拥有，消费者是绑定到 runtime 线程的 service task：

```text
LOG
 -> level check
 -> Abseil format
 -> acquire log_record
 -> bounded MPSC log queue
 -> empty-to-non-empty wake consumer service task
 -> batch drain
 -> backend write
 -> recycle log_record
```

日志对象池不设总容量上限，持续扩展 slab 到 OOM。日志队列可以 bounded，因为后端慢时不能无限吃内存。默认溢出策略建议为 `drop_newest + dropped counter`。

日志生命周期：

```text
created -> accepting -> stopping -> draining -> flushing -> stopped
```

runtime 优雅退出时先停止业务入口，再 drain 已入队任务，随后停止日志接收、drain 日志队列、flush 后端、注销 Abseil sink，最后停止 executor。

## 性能原则

- 调度统一使用 intrusive MPSC，减少多套路径带来的顺序语义差异。
- 对象池快路径走线程局部 cache，跨线程释放批量回收。
- 高频计数器、队列头尾、状态位按 cache line 隔离。
- reactor 默认 LT 模式，只在 interest 变化时修改内核状态。
- TCP 读写按预算 drain，避免单连接饿死同线程其他任务。
- 日志 consumer 按 batch 数量或时间预算运行，空队列时 park。
- 网络日志后端可把真正 TCP/UDP 发送调度到 IO reactor 线程。

## 当前实现与目标差异

当前代码仍有部分历史路径，例如 local/SPSC 调度、更多 `ThreadKind` 类型、部分日志 SPSC lane、网络 command queue 等。后续重构应以本文档和 [docs/next_runtime_architecture.md](docs/next_runtime_architecture.md) 为目标，逐步迁移旧接口、删除兼容层并更新示例和测试。
