# AsyncFlow 下一代 Runtime 架构设计

本文档整理下一代 AsyncFlow 架构方案，覆盖 runtime、task、对象池、timer、reactor、net、logger、配置和优雅退出。目标是高性能、低耦合、职责清晰，并让 API 对用户足够直接。

## 设计原则

- C++17。
- 配置使用普通 `struct`，不使用 builder 链式构造。
- 类、函数、枚举值、变量使用 `lower_case`；成员变量使用尾部 `_`。
- 线程类型只保留 `io` 和 `cpu`。
- 不使用 local queue，不使用 SPSC queue。
- 每个 executor 一个 intrusive unbounded MPSC task inbox。
- 对象池持续扩展 slab，除非系统内存耗尽。
- reactor 统一抽象 epoll/kqueue/select。
- 日志消费者绑定 runtime 线程，作为 service task 运行。
- 网络 fd 操作只在 owner reactor 线程执行，跨线程操作通过 task 显式调度。

## 顶层结构

```text
runtime
  config
  executor[]
    thread_ref
    kind: io/cpu
    task_inbox
    timer_wheel
    reactor, only io executor
    service_tasks
  task_pool
  timer_pool
  log_record_pool
  logger
```

`runtime` 提供基础运行能力。`logger`、`tcp_server`、`udp_socket`、metrics、trace 等组件使用 runtime 的 task、timer、reactor 能力，但不侵入 executor 主循环。

## 配置模型

配置是可直接读写的普通结构体：

```cpp
af::runtime_config cfg;

cfg.threads = {
    af::io_threads("io", 4),
    af::cpu_threads("cpu", 8),
};

cfg.scheduler.task_drain_budget = 256;
cfg.task_pool.local_cache_size = 256;
cfg.logger = af::log_config::ordered();
cfg.logger.consumer_thread = af::thread_selector::cpu(0);
cfg.logger.backends = {
    af::file_log_backend_config{"server.log"},
};

af::runtime rt(cfg);
```

命名工厂函数只生成配置值，不隐藏复杂运行逻辑：

```cpp
af::io_threads("io", 4);
af::cpu_threads("cpu", 8);
af::log_config::ordered();
af::tcp_endpoint::any(8080);
af::tcp_endpoint::any_v6(8080);
```

### runtime_config

```cpp
struct runtime_config {
    thread_layout_config threads;
    scheduler_config scheduler;
    task_pool_config task_pool;
    timer_config timer;
    reactor_config reactor;
    log_config logger;
    shutdown_config shutdown;
    diagnostics_config diagnostics;
};
```

普通用户通常只需要配置 `threads`、`logger` 和业务 server listener。高级用户再调整 scheduler、pool、timer、reactor 的预算和策略。

### thread_layout_config

线程配置需要表达名称、数量、类型和可选系统属性：

```cpp
struct thread_group_config {
    std::string name;
    thread_kind kind;
    std::size_t count;
    thread_affinity_config affinity;
    thread_priority_config priority;
    bool set_os_thread_name = true;
};
```

推荐入口：

```cpp
cfg.threads = {
    af::io_threads("io", 4),
    af::cpu_threads("logic", 8),
};
```

`thread_kind` 只保留：

```cpp
enum class thread_kind {
    io,
    cpu,
};
```

epoll/kqueue/select 属于 `reactor_backend`，不属于线程类型。

当前实例 runtime 在 executor 线程入口会应用 thread layout 的系统属性：`diagnostics.enable_thread_name` 和 group 的 `set_os_thread_name` 控制线程命名；Linux 上非空 `affinity.cpu_ids` 会通过 `pthread_setaffinity_np` 绑定当前 runtime 线程。`thread_priority_config` 目前只保留配置和解析结果，后续需要补充明确的调度策略语义后再应用，避免不同平台上优先级含义不一致。

实例 runtime 解析 thread layout 后会提供轻量 `thread_ref` / `thread_group_ref`：`rt.io_threads()` 和 `rt.cpu_threads()` 返回只读连续 view，不分配、不持有、不加锁；`thread_group_ref::shard(key)` 可按业务 key 选择稳定线程。task 调度、`runtime.post()` 等入口接受 `thread_ref`，为后续 `tcp_server`、`udp_socket` 直接绑定 runtime IO 线程组提供低耦合 API。

### scheduler_config

```cpp
struct scheduler_config {
    std::size_t task_drain_budget = 256;
    std::size_t service_task_budget = 32;
    std::chrono::nanoseconds max_task_run_slice = 0ns;
    idle_wait_strategy idle_wait = idle_wait_strategy::futex;
    wake_policy wake = wake_policy::empty_to_non_empty;
};
```

`max_task_run_slice = 0ns` 表示只按数量预算，不按时间片强制切换。CPU 线程空闲时按 `idle_wait` 选择 futex park、spin 或 yield；IO 线程空闲时由 reactor timeout 和 wake fd 负责等待。

### task_pool_config

```cpp
struct task_pool_config {
    std::size_t local_cache_size = 256;
    std::size_t slab_object_count = 4096;
    oom_policy oom = oom_policy::fatal;
    bool enable_stats = true;
};
```

对象池不设置总容量上限。只要系统还能分配内存，就继续申请新的 slab。需要可恢复失败的业务可使用 `try_make_task<T>()`。

当前实例 runtime 的 typed task pool 仍基于模板化 `ObjectPool`，以保持 slot 布局、local cache 和 remote release 批量参数在编译期固定。`task_pool.slab_object_count` 已作为每个 Task 类型静态池的 reserve-at-least 初始预热容量生效，`task_pool.oom` 已作用于 `make_task<T>()` 的对象池分配失败；用户构造函数抛出的异常不会被改写为对象池 OOM。日志 record pool 已接入 `logger.record_pool.slab_object_count` 作为每个日志 lane/shard 的初始 slab 容量。`local_cache_size`、真正 per-runtime task slab 大小和统计开关还需要后续引入 runtime-parameterized pool 或 size-class pool 后完整接入。

### timer_config

```cpp
struct timer_config {
    timer_kind kind = timer_kind::min_heap;
    std::chrono::milliseconds tick = 1ms;
    std::size_t wheel_slots = 4096;
    std::size_t drain_budget = 256;
    std::size_t initial_reserve = 1024;
};
```

每个 executor 自己拥有 timer 结构。跨线程注册定时器时，先把 task 以 `TimerArming` 状态投递到目标 executor 的同一个 intrusive MPSC inbox，再由目标 executor 在线程内挂入本地 timer 结构，因此 timer 数据结构不需要锁。

当前实现先使用每 executor 本地 min-heap，按 deadline 和 arm sequence 排序；`timer_kind::hierarchical_wheel` 仅作为后续 backend 目标保留，当前配置为该值会校验失败，避免用户误以为已经启用时间轮。后续可以在不改变 task API 和 executor 主循环的前提下，把 heap backend 替换成分层时间轮。

### reactor_config

```cpp
struct reactor_config {
    reactor_backend backend = reactor_backend::auto_select;
    std::size_t event_capacity = 1024;
    std::size_t event_budget = 1024;
    bool edge_triggered = false;
};
```

默认 Linux 使用 epoll，macOS/BSD 使用 kqueue，必要时 fallback 到 select。默认使用 LT 模式。`event_capacity` 控制后端一次可缓存的事件数组容量，`event_budget` 控制单轮 `poll()` 最多分发的 readiness 事件数，用于限制 IO executor 在 fd 事件上停留的时间，避免饿死 task、timer 和 service task。

### log_config

```cpp
struct log_config {
    log_ordering ordering = log_ordering::ordered;
    thread_selector consumer_thread = thread_selector::cpu(0);
    std::size_t queue_capacity = 1U << 16U;
    std::size_t max_batch_records = 256;
    std::chrono::microseconds max_batch_delay = 1000us;
    log_overflow_policy overflow = log_overflow_policy::drop_newest;
    log_record_pool_config record_pool;
    std::vector<log_backend_config> backends;
};
```

日志等级过滤必须在格式化前完成。不匹配等级时不格式化用户消息。

### shutdown_config

```cpp
struct shutdown_config {
    std::chrono::seconds drain_timeout = 5s;
    std::chrono::seconds connection_close_timeout = 5s;
    std::chrono::seconds log_flush_timeout = 5s;
    bool stop_accept_first = true;
};
```

三个 timeout 字段必须为非负值；`0s` 表示进入对应阶段但不额外等待，负值在
`validate_runtime_config()` 阶段直接拒绝。

### diagnostics_config

```cpp
struct diagnostics_config {
    bool enable_task_id = true;
    bool enable_stats = true;
    bool enable_thread_name = true;
    bool enable_queue_metrics = true;
};
```

`enable_task_id` 控制实例 task id 分配和日志 `[task=...]` 注入；`enable_thread_name` 控制实例 runtime 是否调用系统线程命名接口。极致热路径或嵌入式场景可以按需关闭诊断能力。

## Executor 模型

每个 executor 对应一个框架线程和一个 task inbox：

```text
intrusive_unbounded_mpsc_queue<task>
```

CPU executor 循环：

```text
drain task queue with budget
run due timers with budget
run service tasks with budget
empty -> futex park or timed futex wait until nearest timer
```

IO executor 循环：

```text
drain task queue with budget
run due timers with budget
run service tasks with budget
reactor.poll(timeout)
handle io events
empty -> reactor/futex park
```

service task 是长期服务对象，例如 logger、metrics、trace。executor 只知道 service task 的通用接口，不知道 logger 内部细节。

当前静态 runtime 和实例 runtime 都已具备通用 service task 骨架：service 对象通过 `register_service_task()` 在 owner runtime 线程注册，executor 每轮按 `service_task_budget` 调用 `run_service()`；外部生产者只更新 service 自己的 pending 状态并调用 `wake_service_tasks()` 唤醒目标 executor。service 列表仅在 owner runtime 线程访问，因此不需要互斥锁。实例 runtime 的 executor 已按 `scheduler.task_drain_budget` 限制每轮 task inbox drain 数量，按 `timer.drain_budget` 限制每轮到期 timer 数量，并在 `scheduler.max_task_run_slice` 非零时额外按时间片提前让出，避免长任务流饿死 timer、service task 和 reactor poll。静态 runtime 的日志消费者已经迁移到这个通用 service task 接口；实例 runtime 已支持从 `runtime_config.logger` 自动创建 runtime-owned async logger，日志消费者绑定到配置指定的 runtime 线程。

## Task API 与生命周期

任务只能通过 runtime 创建：

```cpp
auto task = af::make_task<login_task>(rt);
task->do_it(conn, req);
```

`make_task<T>()` 默认保证返回非空任务对象或抛出。对象池耗尽时继续扩展 slab；真正 OOM 时按配置执行 fatal 或 throw。当前静态 runtime 实现返回 `TaskHandle<T>`，实例 runtime 实现返回 `runtime_task_handle<T>`；两者都支持 `operator->`，用于持有任务启动前后的生命周期引用。handle 为空只会出现在可恢复创建 API。

需要可恢复失败时使用：

```cpp
auto task = Runtime::try_make_task<login_task>();
if (!task) {
    // recover or reject request
}
```

调度 API：

```cpp
schedule_to(thread_ref target);
schedule_after(duration delay);
schedule_after(thread_ref target, duration delay);
schedule_at(time_point time);
schedule_at(thread_ref target, time_point time);
pending_after(duration delay);
pending_after(thread_ref target, duration delay);
pending_at(time_point time);
pending_at(thread_ref target, time_point time);
reschedule();
done();
cancel();
```

用户不用关心首次 `do_it()` 和后续 `run()` 的内部差异。task 内部可以维护状态机，但用户看到的语义只有“下一次在哪个线程、哪个时间继续执行”。

默认每个 task 创建时分配 `task_id_`。启用 `diagnostics.enable_task_id` 时使用每线程 id block，避免每次创建都访问全局 atomic；禁用时 task id 保持 invalid，日志也不会注入 `[task=...]`，用于对极致热路径关闭诊断开销。

## Task Pool

对象池路径：

```text
local cache
 -> remote free queue drain
 -> slab refill
 -> system allocate slab
 -> OOM policy
```

要求：

- 当前 executor 分配走 local cache，无锁。
- 跨线程释放进入 owner remote free MPSC。
- owner 批量 drain remote free，再回收到 local cache。
- task、log record、timer node、net buffer 分不同 pool 或 size class。
- 高频字段、统计计数器、队列头尾按 cache line 隔离。

对象池不承担背压职责。是否允许任务无限积压由业务入口和调度策略控制，不由 task pool 人为设置固定容量。

## Timer

每个 executor 独立管理 timer。`schedule_after()`、`schedule_at()`、`pending_after()` 和 `pending_at()` 会把 task 挂到目标 executor 的 timer 结构。跨线程调用时不直接修改目标 timer，而是先投递到目标 executor 的 inbox，目标线程再完成 timer arm。

当前状态机：

```text
Created/Pending -> TimerArming -> TimerPending -> Queued -> Starting -> Running
```

`TimerArming` 表示 task 已经进入目标 executor inbox，等待目标线程挂 timer。`TimerPending` 表示 task 已在目标 executor 本地 timer heap 中。timer 到期后目标 executor 将 task 转为 `Queued` 并直接执行。`StopImmediately` 退出时会取消仍在 timer heap 中的 task 并释放生命周期引用。

IO executor 的 `reactor.poll(timeout)` 使用最近 timer 的到期时间作为 timeout。这样不会额外创建 timer 线程，也不会忙等。

## Reactor

每个 IO executor 拥有一个 reactor：

```text
io executor N -> reactor N -> platform backend
```

统一接口：

```cpp
class reactor {
public:
    bool add(fd_event_source* source);
    bool mod(fd_event_source* source);
    bool del(fd_event_source* source);
    poll_result poll(duration timeout);
    void wake();
};
```

`fd_event_source` 表示一个 fd 事件源：

```cpp
struct fd_event_source {
    int fd;
    io_interest interest;
    io_events ready;
    void* owner;
    fd_event_callback callback;
};
```

TCP/UDP/Unix socket 只依赖 reactor 抽象，不直接依赖 epoll/kqueue/select。

当前实例 runtime 已经接入基础 reactor 抽象：只有 IO executor 在构造时创建 reactor，CPU executor 继续使用 futex/atomic wait。`runtime::post()` 和 `stop()` 会同时唤醒目标 executor 的 task inbox 与 reactor wake fd，保证 IO 线程阻塞在 `poll(timeout)` 时也能及时处理被调度过来的 task、timer arm 和停止请求。当前新路径已经拆分 public reactor 接口与后端实现，并落地 Linux epoll、macOS/BSD kqueue 和 select fallback；`auto_select` 在 Linux 优先 epoll，在 macOS/BSD 优先 kqueue，最后 fallback 到 select。epoll/kqueue 在系统调用入口按 `reactor.event_budget` 限制单轮返回事件数，select fallback 在 ready fd 收集阶段按同一预算截断。

## 网络层

网络层对象：

```text
tcp_server
tcp_listener
tcp_connection
tcp_client
udp_socket
unix_listener
unix_connection
```

所有真实 fd 操作都必须在 owner IO executor 上执行。跨线程操作通过 task 显式 `schedule_to(owner_thread)`。

### TCP Server 配置

```cpp
struct tcp_server_config {
    tcp_connection_config connection;
    std::chrono::milliseconds connection_close_timeout = 5000ms;
};

struct tcp_listener_config {
    tcp_endpoint endpoint;
    thread_group_ref threads;
    bool reuse_addr = true;
    bool reuse_port = false;
    int backlog = 4096;
    std::size_t accept_budget = 256;
    bool ipv6_only = true;
    tcp_handler* handler = nullptr;
};

struct tcp_connection_config {
    std::size_t read_buffer_size = 16U * 1024U;
    std::size_t read_budget_bytes = 512U * 1024U;
    std::size_t write_budget_bytes = 512U * 1024U;
    std::size_t output_high_watermark = 8U * 1024U * 1024U;
    bool no_delay = true;
    bool keepalive = true;
    std::chrono::seconds idle_timeout = 0s;
};
```

使用方式：

```cpp
af::tcp_server_config server_cfg;
server_cfg.connection.no_delay = true;
server_cfg.connection.keepalive = true;

af::tcp_server server(rt, server_cfg);

af::tcp_listener_config listener;
listener.endpoint = af::tcp_endpoint::any(8080);
listener.threads = rt.io_threads();
listener.reuse_port = true;
listener.backlog = 4096;
listener.handler = &handler;

server.add_listener(listener);
server.start();
```

`tcp_server` 支持多个 listener 和运行中 `add_listener()`。如果需要从外部线程操作 server，用户显式创建 task 并调度到目标 IO 线程。

### TCP 流程

```text
start on io thread
create nonblocking listen fd
bind/listen
reactor.add(listener fd)
listener readable
accept loop
create tcp_connection
reactor.add(connection fd)
connection readable
recv loop
handler.on_read(conn_ref, bytes)
business task schedule_to(cpu)
business done schedule_to(conn.owner_thread())
conn.send(response)
```

默认 LT 模式。读事件 drain 到 `EAGAIN` 或预算耗尽。写事件只在 output buffer 非空时开启，写完后关闭 writable interest。

### Connection 引用

```cpp
tcp_connection_ref
tcp_connection_handle
```

`tcp_connection_ref` 只在 owner IO 线程回调内有效。`tcp_connection_handle` 可跨线程保存，包含 owner thread、slot、generation。旧 handle 不应误发到复用后的新连接。

## UDP 与 Unix Socket

UDP 使用 `udp_socket` 抽象，server 和 client 共用 datagram 模型：

```cpp
struct udp_socket_config {
    udp_endpoint endpoint;
    thread_group_ref threads;
    bool reuse_addr = true;
    bool reuse_port = false;
    std::size_t recv_budget_packets = 256;
    std::size_t recv_buffer_size = 2048;
    std::size_t send_queue_high_watermark = 4U * 1024U * 1024U;
};
```

Unix stream 和 Unix datagram 使用独立 API，避免把 Unix path 混入 TCP/UDP endpoint 心智模型。底层可复用 stream/datagram 热路径。

## Logger

logger 由 runtime 拥有，消费者是绑定到 runtime 线程的 service task，不创建独立线程，也不创建长期 consumer task。

当前实例 runtime 已提供两类启动方式：

```cpp
af::runtime runtime(cfg); // cfg.logger.backends 非空时由 runtime.start() 自动启动
auto logging = af::start_async_logging_for_runtime(runtime, async_log_config); // 手动模式
```

自动模式把 `runtime_config.logger` 转换为 `AsyncLogConfig`，按 `consumer_thread` 选择目标 runtime 线程，并把消费者注册为该 executor 上的 service task。没有配置 backend 时不创建全局 Abseil sink。`runtime.stop()` 会先停止日志 admission、drain 已接受记录并 flush backend，再停止 executor；手动模式仍保留给需要自定义 `AsyncLogConfig` 或临时日志管线的场景。

前端流程：

```text
LOG
 -> level check
 -> Abseil format
 -> acquire log_record
 -> push bounded MPSC log queue
 -> if empty-to-non-empty, wake consumer
```

消费者流程：

```text
consumer service task
 -> drain N records or run T duration
 -> write backend batch
 -> recycle log_record
 -> if queue still non-empty, report did_work and let executor continue polling
 -> else park
```

日志队列建议 bounded，默认 `drop_newest + dropped counter`。日志对象池不 bounded，持续扩展 slab 到 OOM。

### 日志后端

文件后端：

```cpp
struct file_log_backend_config {
    std::string path;
    bool append = true;
    bool fsync_on_flush = false;
    std::size_t write_batch_iov = 64;
};
```

UDP/TCP 后端：

```cpp
struct udp_log_backend_config {
    endpoint target;
    std::size_t max_datagram_size = 1400;
    thread_selector io_thread;
};

struct tcp_log_backend_config {
    endpoint target;
    std::chrono::milliseconds reconnect_interval = 500ms;
    thread_selector io_thread;
};
```

网络日志后端可以由日志消费者聚合 batch，再调度到指定 IO executor，通过 reactor 发送。当前 `runtime_config.logger` 的 `io_thread` 已参与配置校验；实际 file/udp/tcp 后端先复用现有 `LogBackend` 实现，网络后端完全 reactor 化发送仍在后续迁移项中。
UDP/TCP 日志后端的 `io_thread` 必须解析到 `thread_kind::io` executor；解析失败或指向
CPU 线程都会在 `validate_runtime_config()` 阶段拒绝。

### 日志生命周期

```text
created
 -> accepting
 -> stopping
 -> draining
 -> flushing
 -> stopped
```

`accepting` 状态接受用户日志。`stopping` 后停止普通日志 admission。`draining` 消费已接受记录。`flushing` 调用后端 flush。`stopped` 注销 sink 并释放资源。

当前 runtime-owned logger 在 `runtime.stop()` 时会使用 `shutdown.log_flush_timeout` 作为日志消费者 drain/flush 的等待预算；手动 `AsyncLogHandle::stop()` 也提供 timeout 重载。后端自身如果阻塞在不可取消 IO 上，仍需要后端实现自己的 `shutdown()` 解除阻塞。

## 优雅退出

推荐 runtime 停止顺序：

```text
1. stop_accept
2. stop_external_task_submission
3. drain_business_tasks
4. logger.stop_admission
5. logger.drain
6. logger.flush
7. backend.shutdown
8. unregister_absl_sink
9. stop_executors
```

退出过程中普通用户日志在 `logger.stop_admission` 后不再进入队列，避免退出阶段无限产生日志。必要的 runtime 紧急日志可以进入小型 emergency buffer，或者同步写 stderr。

TCP server 停止：

```text
stop accept
remove listener from reactor
close listen fd
close_after_flush active connections
deadline reached -> force close
```

当前模板化 `af::net::tcp_server<Runtime>` 已提供 `tcp_server_config.connection_close_timeout`：`stop()` 会在 owner IO 线程先关闭 listener、对存活连接执行 `close_after_flush()`，连接自然 flush 完会立即结束；超过配置 timeout 后强制关闭剩余连接。`tcp_server_config.connection` 也已作为 listener 默认连接配置接入，覆盖读 buffer、读预算、写预算、输出高水位、`TCP_NODELAY` 和 keepalive；listener options 仍可按监听入口覆盖。实例 runtime 的 `shutdown.connection_close_timeout` 仍待未来网络层迁移到实例 runtime API 后直接接入。

task 不强杀正在运行的业务逻辑；pending task 可以取消，running task 通过 stopping 状态自行收敛。

## 目录建议

```text
include/af/
  runtime/
    config_types.hpp
    config_resolution.hpp
  task/
  queue/
  memory/
  timer/
  reactor/
  net/
    tcp/
    udp/
    unix/
  log/
  platform/
```

平台后端：

```text
include/af/reactor/epoll_reactor.hpp
include/af/reactor/kqueue_reactor.hpp
include/af/reactor/select_reactor.hpp
```

## 与当前实现的主要差异

- 当前调度已经统一为每 executor 一个 intrusive MPSC task inbox，后续继续收敛配置 API 和命名。
- 当前线程类型已只保留 `thread_kind::io` 和 `thread_kind::cpu`，epoll/kqueue/select 属于 reactor backend。
- 当前旧网络 readiness 路径已使用 LT 语义，网络 channel 不使用 one-shot rearm；新的实例 runtime 已提供 `reactor` / `fd_event_source` 抽象、Linux epoll backend、macOS/BSD kqueue backend 和 select fallback，IO executor 可在 `reactor.poll(timeout)` 中同时等待 fd readiness、task wake 和 timer deadline，并已按 `reactor.event_budget` 限制单轮 readiness 分发量。
- 当前日志 relaxed 模式已使用 bounded MPSC runtime lane 和 sharded MPSC ingress，日志 record pool 已改为可扩展 slab pool，并已按 `logger.record_pool.slab_object_count` 控制每个日志 lane/shard 的初始 slab 容量。
- 当前 TCP stream 和 UDP/datagram 跨线程操作已迁移为显式 runtime task 调度到 owner reactor；后续继续收敛 API 命名、目录结构和对象池实现。
- 当前 runtime 已提供 `try_make_task<T>()` 可恢复创建路径；对象池 `try_create()` 在分配或构造失败时返回空指针，并释放已获取 slot；实例 runtime 的 `make_task<T>()` 已接入 `task_pool.slab_object_count` 预热和 `task_pool.oom` 分配失败策略。
- 当前 executor 已提供通用 service task 注册、注销和唤醒入口；runtime async logger 消费者已迁移为 service task，只有注册/注销使用短 control task 切到 owner executor。
- 当前日志队列仍是有界队列，record 对象池不再用固定总容量承担背压职责。
- 当前网络 public API 已补 lower_case 类型别名和 enum 值别名，例如 `tcp_server`、`tcp_client`、`udp_socket`、`send_result::backpressure`、`close_reason::error` 和 Unix socket 相关别名；TCP echo/login 示例已使用新命名作为迁移样板。
- 当前 task API 已补 `schedule_to(...)` / `pending_to(...)` 及 fast/ordered 变体作为清晰调度入口，旧 `schedule(...)` / `pending_on(...)` 保留兼容。
- 当前 runtime/task public enum 与容器已补 lower_case 别名，例如 `task_result`、`schedule_mode::ordered`、`shutdown_policy::wait_for_tasks`、`parallel_mode::non_empty_only` 和 `sharded_ops<T>`；示例已迁移到新拼写。
- 当前已提供 public `runtime_config`、`scheduler_config`、`task_pool_config`、`timer_config`、`reactor_config`、`log_config`、`shutdown_config` 和 `diagnostics_config` 普通结构体，`io_threads()` / `cpu_threads()` 配置值工厂，以及 `resolve_runtime_config()` / `validate_runtime_config()` 解析校验入口。
- 当前已提供 `af::runtime` 实例生命周期外壳和低层投递通道：构造时使用结构化配置解析校验，`start()` 按配置启动 runtime 线程，每个 executor 拥有 intrusive MPSC inbox，`post()` 可把 `runtime_work` 投递到指定线程执行，每轮按 `scheduler.task_drain_budget` drain task、按 `timer.drain_budget` drain 到期 timer，且在 `scheduler.max_task_run_slice` 非零时按时间片让出；`scheduler.wake` 已控制投递唤醒策略，默认 empty-to-non-empty 通过消费者 sleep flag 减少重复 wake，并在真正 wait 前复查队列/定时器/service task；CPU executor 空闲等待已支持 `scheduler.idle_wait` 的 futex/spin/yield 策略，IO executor 使用 reactor poll；`stop()` 可由外部线程完成 join/回收，也可由 runtime 线程内请求停止而不自 join。
- 当前实例 runtime 已接入 `diagnostics.enable_thread_name`：开启时按 thread layout 调用系统线程命名接口，关闭时跳过命名调用；单个 thread group 仍可通过 `set_os_thread_name=false` 禁用自身命名。
- 当前实例 runtime 已在 Linux 上接入 `thread_group_config.affinity.cpu_ids`：executor 线程入口对当前线程调用 `pthread_setaffinity_np`；非 Linux 平台当前为 no-op。`thread_priority_config` 仍处于配置解析阶段。
- 当前实例 runtime 已提供 service task 注册、注销和唤醒入口：service 列表只在目标 executor 线程内修改，executor 主循环按 `scheduler.service_task_budget` 轮询，外部唤醒不加锁。
- 当前实例 runtime 已提供 runtime-owned async logger：`runtime.start()` 在 `runtime.config().logger.backends` 非空时自动构造 file/udp/tcp 后端并把消费者注册为 service task，`runtime.stop()` 先 drain/flush 日志再停止 executor；`start_async_logging_for_runtime(runtime&, config, thread)` 仍作为手动自定义入口保留。
- 当前 runtime-owned async logger 已接入 `shutdown.log_flush_timeout`，用于控制停止时日志 drain/flush 的等待预算；模板化 `tcp_server` 已支持自身 `tcp_server_config.connection_close_timeout` 和 server 级默认 `tcp_connection_config`，但实例 `runtime_config.shutdown.connection_close_timeout` 仍待网络层迁移到实例 runtime API 后直连。
- 当前实例 runtime 已提供 `af::runtime_task`、`af::make_task<T>(runtime, ...)`、`af::try_make_task<T>(runtime, ...)` 和 `runtime_task_handle<T>`：任务创建走 typed slab object pool，任务 id 默认使用每线程 block 分配，`diagnostics.enable_task_id=false` 时跳过 id 分配并保持 invalid；`schedule_to()` 投递到目标 executor，运行中请求下一跳会延后到当前 `run_task()` 返回后再入队，避免同一个任务对象并发重入。
- 当前实例 runtime 已提供 `thread_ref` / `thread_group_ref`：`runtime::io_threads()`、`runtime::cpu_threads()` 返回已解析线程索引的连续 view，`thread_group_ref::shard()` 用于稳定分片选择；`runtime::post()` 和 `runtime_task` 调度入口已支持 `thread_ref`，便于后续网络层直接使用 runtime thread group。
- 当前实例 runtime 已接入每 executor 本地 timer min-heap：`schedule_after()` / `schedule_at()` / `pending_after()` / `pending_at()` 先把 task 以 `timer_arming` 状态投递到目标 inbox，目标线程再挂入本地 heap；executor 空闲等待使用最近 timer deadline 作为 atomic/futex timeout，IO executor 则把最近 timer deadline 传给 `reactor.poll(timeout)`；`stop()` 会取消未到期 timer 并释放 task 生命周期引用。后续可在不改变 task API 的前提下把 min-heap backend 替换为分层时间轮。

后续迁移应先补齐测试和 benchmark，再逐步替换旧路径，避免一次性重写导致行为不可控。
