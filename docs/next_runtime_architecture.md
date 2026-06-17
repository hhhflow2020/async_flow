# AsyncFlow 下一代 Runtime 架构设计

本文档是 AsyncFlow 下一代 runtime、task、timer、reactor、network 和 logger 的目标架构蓝图。它描述后续重构和实现应对齐的设计，不把历史兼容层作为目标形态。

## 目标

- C++17 标准，允许使用 C++17 及以上编译器编译。
- conan 管理第三方依赖：`abseil/20260107.1`、`gtest/1.17.0`、`benchmark/1.9.5`、`protobuf/7.35.0`、`folly/2024.08.12.00`。
- 使用现代 CMake：target 级 include、compile feature、link library 和测试/benchmark 目标。
- 类、函数、枚举值和变量使用 `lower_case`；成员变量额外使用尾部 `_`。
- 配置使用普通 `struct`，允许少量命名工厂函数，不使用 builder 链式构造。
- 线程类型只保留 `thread_kind::io` 和 `thread_kind::cpu`。
- IO 后端只保留 `epoll`、`kqueue`、`select` 三类 reactor backend；不保留 `io_uring` 路径和兼容桩。
- 当前只在 `af::buffer` 使用 Folly `IOBuf`；Linux 上 ConanCenter 的完整 `folly` recipe 会传递构建 `liburing` 和 Folly 自身 async io_uring 源文件。AsyncFlow 框架代码仍不提供 io_uring backend；如果依赖层也必须完全不出现 liburing，需要后续改为 iobuf-only 自建包或定制 recipe。
- 每个 executor 一个 intrusive unbounded MPSC task inbox；不使用 local queue，不使用 SPSC queue。
- CPU executor 空闲等待使用 futex/atomic wait，IO executor 空闲等待进入 reactor poll。
- 对象池是高性能分配器，不承担固定容量背压；除非系统内存耗尽，否则持续扩展 slab。
- 日志消费者绑定 runtime 线程，以 service task 方式运行，不创建独立 consumer 线程。
- 网络 fd 的真实操作只在 owner reactor 线程执行；跨线程控制和发送通过 task 显式调度到 owner 线程。
- 默认网络 readiness 使用 LT 模式，避免 one-shot 每次事件后重新注册带来的系统调用开销。

## 总体结构

```text
af::runtime
  runtime_config
  executor[]
    thread_ref
    thread_kind: io/cpu
    intrusive_mpsc_task_inbox
    timer_backend
    reactor, only io executor
    service_task list
    executor_local_cache
  task_pool
  timer_pool
  log_record_pool
  logger
```

`runtime` 只提供通用执行能力：线程、任务、定时器、reactor、service task、对象池和生命周期。网络、日志、metrics、trace 等组件建立在这些能力上，不把组件专属逻辑写死到 executor 主循环。

## 配置模型

配置是普通数据对象，用户可以直接读写字段：

```cpp
af::runtime_config cfg;

cfg.threads = {
    af::io_threads("io", 4),
    af::cpu_threads("logic", 8),
};

cfg.scheduler.task_drain_budget = 256;
cfg.scheduler.service_task_budget = 32;
cfg.task_pool.local_cache_size = 256;
cfg.timer.kind = af::timer_kind::hierarchical_wheel;
cfg.reactor.backend = af::reactor_backend::auto_select;

cfg.logger = af::log_config::ordered();
cfg.logger.consumer_thread = af::thread_selector::cpu(0);
cfg.logger.backends = {
    af::file_log_backend_config{.path = "server.log"},
};

af::runtime rt(cfg);
```

命名工厂函数只负责生成配置值，不隐藏运行逻辑：

```cpp
af::io_threads("io", 4);
af::cpu_threads("logic", 8);
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

普通用户通常只需要配置 `threads`、`logger` 和业务网络入口。高级用户再调整 scheduler、pool、timer、reactor 的预算和策略。

### thread_layout_config

线程配置表达名称、数量、类型和可选系统属性：

```cpp
enum class thread_kind {
    io,
    cpu,
};

struct thread_group_config {
    std::string name;
    thread_kind kind = thread_kind::cpu;
    std::size_t count = 1;
    thread_affinity_config affinity;
    thread_priority_config priority;
    bool set_os_thread_name = true;
};
```

runtime 解析配置后提供轻量 view：

```cpp
af::thread_group_ref io = rt.io_threads();
af::thread_group_ref logic = rt.thread_group("logic");
af::thread_ref owner = io.shard(user_id);
```

`thread_ref` 只保存 executor index。`thread_group_ref` 只保存 begin/count/runtime 指针或等价轻量引用；`at()` 和 `shard()` 是整数运算，不加锁、不分配。

线程启动时可按配置调用系统接口设置线程名、CPU affinity 和 nice priority。非 Linux 平台不支持的属性保持 no-op，不能把不同平台的实时调度策略混在同一个字段里。

### scheduler_config

```cpp
struct scheduler_config {
    std::size_t task_drain_budget = 256;
    std::size_t timer_drain_budget = 256;
    std::size_t service_task_budget = 32;
    std::chrono::nanoseconds max_task_run_slice = 0ns;
    idle_wait_strategy idle_wait = idle_wait_strategy::futex;
    wake_policy wake = wake_policy::empty_to_non_empty;
};
```

`max_task_run_slice = 0ns` 表示只按数量预算，不按时间强制切换。CPU executor 空闲时按 `idle_wait` 选择 futex park、spin 或 yield；IO executor 空闲时由 reactor timeout 和 wake fd 统一等待。

### task_pool_config

```cpp
struct task_pool_config {
    std::size_t local_cache_size = 256;
    std::size_t slab_object_count = 4096;
    oom_policy oom = oom_policy::fatal;
    bool enable_stats = true;
};
```

对象池不设置总容量上限。只要系统还能分配内存，就继续申请新的 slab。`make_task<T>()` 默认返回非空指针或按 OOM 策略终止/抛出；可恢复业务使用 `try_make_task<T>()`。

### timer_config

```cpp
struct timer_config {
    timer_kind kind = timer_kind::hierarchical_wheel;
    std::chrono::milliseconds tick = 1ms;
    std::size_t wheel_slots = 4096;
    std::size_t drain_budget = 256;
    std::size_t initial_reserve = 1024;
};
```

默认使用分层时间轮，适合大量 timer。小规模或需要严格 heap 语义的测试场景可以选择 `timer_kind::min_heap`。无论哪种 backend，都只由 owner executor 修改，因此 timer 数据结构不需要 mutex。

### reactor_config

```cpp
struct reactor_config {
    reactor_backend backend = reactor_backend::auto_select;
    std::size_t event_capacity = 1024;
    std::size_t event_budget = 1024;
    bool edge_triggered = false;
};
```

`auto_select` 在 Linux 选择 epoll，在 macOS/BSD 选择 kqueue，其他 POSIX 平台 fallback 到 select。默认 LT 模式。`event_budget` 限制单轮 readiness 分发数量，避免 IO executor 被 fd 事件长期占满。

### log_config

```cpp
struct log_config {
    log_ordering ordering = log_ordering::ordered;
    log_level min_level = log_level::info;
    thread_selector consumer_thread = thread_selector::cpu(0);
    std::size_t queue_capacity = 1U << 16U;
    std::size_t shard_count = 0;
    std::size_t max_batch_records = 256;
    std::chrono::microseconds max_batch_delay = 1000us;
    log_overflow_policy overflow = log_overflow_policy::drop_newest;
    log_record_pool_config record_pool;
    std::vector<log_backend_config> backends;
};
```

日志等级过滤必须发生在格式化之前。不匹配等级时不格式化用户消息，也不申请 `log_record`。用户侧日志入口使用 `AF_LOG(INFO)` / `AF_LOG_IF(INFO, condition)`，该入口先检查 `log_config::min_level` 或运行时 `set_min_log_level()` 维护的前端等级，再进入 Abseil stream 格式化。

### shutdown_config

```cpp
struct shutdown_config {
    std::chrono::seconds drain_timeout = 5s;
    std::chrono::seconds connection_close_timeout = 5s;
    std::chrono::seconds log_flush_timeout = 5s;
    bool stop_accept_first = true;
};
```

timeout 必须非负。`0s` 表示进入对应阶段但不额外等待。

## Executor 模型

每个 executor 对应一个固定框架线程和一个 task inbox：

```text
intrusive_unbounded_mpsc_queue<task>
```

CPU executor 循环：

```text
drain task inbox with budget
run due timers with budget
run service tasks with budget
if no work:
  futex/atomic wait until wake or nearest timer
```

IO executor 循环：

```text
drain task inbox with budget
run due timers with budget
run service tasks with budget
compute timeout from nearest timer and service need
reactor.poll(timeout)
dispatch readiness events with budget
```

task、timer、service task 和 reactor event 都有预算。这样 IO 线程既能处理 fd 事件，也能处理调度到本线程的 task，同时避免某一类工作长期占满 executor。

same-thread schedule 也进入统一 task inbox。这样所有调度都走同一种顺序语义，不再因为 local queue、SPSC queue、MPSC queue 混用而产生难以解释的执行顺序差异。

## Task API 与生命周期

任务只能通过 runtime 创建：

```cpp
auto* task = af::make_task<login_task>(rt);
task->do_it(conn, request);
```

`make_task<T>()` 返回非空 `T*`。对象池分配失败时按 `task_pool.oom` 处理，不把普通用户路径变成到处判断空指针。需要可恢复失败时使用：

```cpp
if (auto* task = af::try_make_task<login_task>(rt)) {
    task->do_it(conn, request);
}
```

task 启动是两步：先创建对象，再由用户调用 `do_it()` 填充上下文并发起第一次调度。用户不需要关心首次 `do_it()` 和后续 `run()` 在 runtime 内部的状态差异；用户只表达任务下一次在哪个线程、哪个时间继续执行。

调度 API 使用 `schedule` 关键词：

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

语义：

- `schedule_to(target)`：下一次继续运行到目标 executor。
- `schedule_after(delay)`：在当前 owner executor 上延迟运行。
- `schedule_after(target, delay)`：投递到目标 executor，由目标 executor 挂入本地 timer。
- `schedule_at(time)` / `schedule_at(target, time)`：绝对时间版本。
- `reschedule()`：回到当前 executor 的 task inbox，让出当前执行机会。
- `done()`：任务完成并归还对象池。
- `cancel()`：取消 pending/timer 状态任务；正在运行的业务逻辑不被强杀。

每个 task 创建时分配 `task_id_`。启用 `diagnostics.enable_task_id` 时使用每线程 id block，避免每次创建都访问全局 atomic；禁用时 task id 保持 invalid，日志不会注入 `[task=...]`。

## Queue 与背压

task 投递使用 Vyukov intrusive unbounded MPSC linked queue。队列节点就是 task 自身的 intrusive node，不为每次投递额外分配 wrapper。

```text
producer:
  task.next = nullptr
  prev = exchange(head, task)
  prev.next = task

consumer:
  drain from tail/stub
```

task inbox 不因为容量满而拒绝非首次调度。业务入口的限流、连接高水位、日志队列溢出和内存 OOM 是独立问题，不能让 task 自身的继续调度在“队列满”时陷入复杂恢复逻辑。

日志队列是 bounded MPSC，因为后端慢时不能无限吃内存。默认溢出策略是 `drop_newest + dropped counter`，生产者不阻塞；`block` 只能作为显式配置，用于确实希望日志反压业务的场景。

## 对象池

对象池路径：

```text
thread/executor local cache
 -> drain owner remote free queue in batch
 -> refill from current slab
 -> allocate new slab
 -> OOM policy
```

设计要求：

- 当前 executor 分配走 local cache，无锁。
- 跨线程释放进入 owner remote free MPSC。
- owner 批量 drain remote free，再回收到 local cache。
- task、timer node、log record、net buffer 分类型或 size class 管理。
- 高频字段、统计计数器、队列 head/tail、producer cursor、consumer cursor 按 cache line 隔离。
- slab 内对象连续存放，提升预取和 cache locality。
- 对象池不设置固定总容量；内存耗尽前持续提供对象。

`make_task<T>()`、log record acquire、timer node acquire 都走相同的设计原则，但不要强行共用一个池。不同对象生命周期和访问模式不同，应分 pool 或 size class。

## Timer

每个 executor 拥有自己的 timer backend。跨线程注册 timer 时，不直接修改目标 timer 结构，而是把 task 投递到目标 executor，由目标 executor 在线程内完成 timer arm。

状态流：

```text
created
 -> queued
 -> running
 -> timer_arming
 -> timer_pending
 -> queued
 -> running
 -> done/cancelled
```

IO executor 的 `reactor.poll(timeout)` 使用最近 timer deadline 计算 timeout；CPU executor 使用 futex/atomic timed wait。框架不创建额外 timer 线程。

定时器分两类：

- frame timing wheel：适合游戏 tick、批处理和固定节奏任务。
- hierarchical timing wheel：适合大量普通 timeout，近端 bucket 快速触发，远端 bucket 分层推进。

## Reactor

每个 IO executor 拥有一个 reactor 对象：

```text
io executor 0 -> reactor 0 -> epoll/kqueue/select
io executor 1 -> reactor 1 -> epoll/kqueue/select
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

`fd_event_source` 是内部事件源，不作为业务层概念暴露：

```cpp
struct fd_event_source {
    native_fd fd;
    io_interest interest;
    io_events ready;
    void* owner;
    fd_event_callback callback;
};
```

TCP、UDP、Unix socket 只依赖 reactor 抽象，不直接依赖 epoll/kqueue/select。LT 模式下，读回调 drain 到 `EAGAIN` 或预算耗尽，写 interest 只在输出队列从空变非空、或从非空变空时修改。这样普通 readiness 处理不需要每次事件后重新 `epoll_ctl` / `kevent`。

## 网络层

网络层目标对象：

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

所有 fd 都有明确 owner reactor thread。真实 fd 操作只能在 owner IO executor 上执行。外部线程或 CPU task 要发送、关闭、增加 listener、停止 server 时，应显式创建 task 并 `schedule_to(owner_thread)`。

框架不再提供隐藏 command queue，也不在 release 热路径做“外部线程直接调用拒绝”这类同步检查。使用契约是：涉及 reactor 对象生命周期和 fd 操作的 API 必须在 owner reactor 线程调用；debug 构建可以保留 assert 帮助定位误用。

### TCP Server API

`tcp_server` 不是业务 task。它是控制对象，持有 listeners、connection table 和 server 生命周期状态。listener 和 connection 通过 reactor event source 驱动；业务需要 CPU 计算时再创建 task。

配置示例：

```cpp
af::net::tcp_server_config server_cfg;
server_cfg.connection.no_delay = true;
server_cfg.connection.keepalive = true;
server_cfg.connection.output_high_watermark = 8U * 1024U * 1024U;

af::net::tcp_server server(rt, server_cfg);

af::net::tcp_listener_config public_listener;
public_listener.name = "public";
public_listener.endpoint = af::net::tcp_endpoint::any(8080);
public_listener.threads = rt.io_threads();
public_listener.reuse_addr = true;
public_listener.reuse_port = true;
public_listener.backlog = 4096;
public_listener.accept_budget = 256;
public_listener.handler = &handler;

server.add_listener(public_listener);
server.start();
```

以上控制代码应在相关 owner IO task 中执行。外部线程需要先投递 runtime task 到目标 reactor 线程，再调用 `add_listener()` / `start()` / `stop()`；框架不维护隐藏 command queue，也不在 release 热路径保存固定 control thread 状态。

`threads` 可绑定一个或多个 IO 线程。未填写时默认使用 `rt.io_threads()`。运行中 `add_listener()` 和 `remove_listener()` 支持动态监听地址管理，但调用方需要先显式切到涉及对象所属的 owner IO 线程。

多 IO 线程监听策略：

- `reuse_port=true`：每个目标 IO 线程各自创建 listen fd，内核分配连接，性能最好。
- `reuse_port=false`：目标上可扩展为只在一个 IO 线程创建 listen fd，accepted fd 再按策略分发到目标 IO 线程；当前实现先支持 `reuse_port=true` 的 shard 化高性能路径。
- 同一个 listen fd 不放入多个 reactor；每个 fd 只属于一个 reactor。

TCP server 流程：

```text
control task on io thread
 -> create nonblocking listen fd
 -> setsockopt reuse_addr/reuse_port/ipv6_only
 -> bind/listen
 -> reactor.add(listener source)

listener readable
 -> accept loop until EAGAIN or accept_budget
 -> set accepted fd nonblocking
 -> select owner io thread
 -> create tcp_connection
 -> reactor.add(connection source)

connection readable
 -> recv loop until EAGAIN/read_budget
 -> handler.on_read(conn_ref, bytes)
 -> business may make_task and schedule_to(cpu)

business task done
 -> schedule_to(conn.owner_thread())
 -> conn.send(response)
```

### TCP Connection

```cpp
tcp_connection_ref
tcp_connection_handle
```

`tcp_connection_ref` 只在 owner IO 线程回调内有效，适合直接读写、获取 peer/local endpoint、访问 user context。

`tcp_connection_handle` 可跨线程保存，包含 owner thread、slot、generation。业务层可维护 `user_id -> tcp_connection_handle`，例如登录成功后把用户 id 绑定到连接。owner IO 线程执行 send/close 时校验 generation，避免旧 handle 误发到复用后的新连接。

发送策略：

```cpp
handle.send(buffer);       // 跨线程安全：内部调度到 owner reactor
ref.send(buffer_view);     // owner IO 线程内热路径
handle.close_after_flush();
handle.close_now();
```

写队列只由 owner IO 线程修改。业务线程不直接写 fd。输出 buffer 支持 move buffer、scatter/gather `writev`，可在平台支持时扩展 `sendfile` / `splice` 等零拷贝路径。

### TCP Client

`tcp_client` 是出站连接控制对象。调用方选择 IO 线程，目标 IO 线程创建 nonblocking socket 并执行 `connect`。`EINPROGRESS` 后由 reactor 等待 writable readiness；等待期间 IO 线程继续处理 task、timer 和其他 fd。

连接建立后复用 `tcp_connection` 热路径：读写、关闭、handle generation、输出高水位都与 server accepted connection 一致。

### UDP Socket

`udp_socket` 同时覆盖 UDP server 和 UDP client。server 绑定 local endpoint；client 可绑定 local endpoint 并设置 remote endpoint。

```cpp
af::net::udp_socket_config cfg;
cfg.name = "udp-echo";
cfg.local_endpoint = af::net::udp_endpoint::any(9000);
cfg.threads = rt.io_threads();
cfg.reuse_port = true;
cfg.recv_budget_packets = 256;
cfg.recv_buffer_size = 2048;

af::net::udp_socket socket(rt, cfg, handler);
socket.start();
```

每个 active IO shard 拥有独立 fd、handler 副本、recv buffer 和 event source。`reuse_port=true` 时多 IO 线程各自 bind 同一端口，由内核分发 datagram。跨线程发送通过 `udp_socket_handle::send()` / `send_to()` 调度到目标 shard。

### Unix Socket

Unix stream 使用 `unix_stream_server` / `unix_stream_client`，底层复用 stream connection 热路径。Unix datagram 使用 `unix_datagram_socket`，底层复用 datagram shard 热路径。

Unix path 的 bind、unlink、权限和生命周期与 TCP/UDP endpoint 不同，因此对外 API 独立，不把 path 塞进 TCP/UDP 配置里。

## Packet 与业务分发

框架网络层只提供高性能字节流/datagram 能力，不内置 codec/dispatcher。包格式、包 id、protobuf/json 解析和业务分发由用户组合。

常见 TCP 包格式：

```text
uint32 length
uint16 packet_id
bytes  content
```

业务可以维护 `packet_id -> handler` 表：

```cpp
using packet_handler = void (*)(af::net::tcp_connection_handle conn,
                                af::buffer_view content);

absl::flat_hash_map<std::uint16_t, packet_handler> handlers;
```

TCP IO 线程负责从连接输入 buffer 中解析完整包，拿到完整包后按包 id 创建对应 task。protobuf 只作为 `tcp_login_server` 示例依赖，不进入框架核心。

## Logger

logger 由 runtime 拥有，消费者绑定到 runtime 某个线程，以 service task 方式运行：

```text
AF_LOG
 -> level check
 -> Abseil format
 -> acquire log_record
 -> push bounded MPSC ingress
 -> empty-to-non-empty wake consumer service task
 -> consumer batch drain
 -> backend write
 -> recycle log_record
```

默认 `log_ordering::ordered`。ordered 策略给已接受日志分配递增 sequence，consumer 按 sequence 输出。为了减少多生产者写同一个 cache line，可以使用 sharded bounded MPSC ingress：生产者按线程或 CPU shard 写本地 shard，consumer 小批量 merge。sequence 可按线程块分配，减少每条日志访问全局 atomic。

`log_ordering::relaxed` 是显式可选策略，允许 consumer 按 shard/batch 到达顺序输出，换取更低 merge 成本。ordered 和 relaxed 的代码路径应隔离清楚，配置入口简单：

```cpp
cfg.logger = af::log_config::ordered();
cfg.logger = af::log_config::relaxed();
```

日志队列 bounded，默认满时 `drop_newest` 并增加 dropped counter，避免后端慢时阻塞业务生产者。日志 record pool 不 bounded，持续扩展 slab 到 OOM。

后端：

- file backend：consumer 所在 runtime 线程直接批量写文件。
- UDP backend：consumer 聚合 batch 后调度到目标 IO reactor 发送。
- TCP backend：连接、重连和发送都归属目标 IO reactor。

日志生命周期：

```text
created
 -> accepting
 -> stopping
 -> draining
 -> flushing
 -> stopped
```

`runtime.stop()` 先停止日志 admission，再 drain 已接受记录，最后 flush backend。停止后的普通用户日志不再进入队列，避免退出阶段无限产生日志。

## 优雅退出

推荐停止顺序：

```text
1. stop accepting external work
2. stop tcp/unix listeners and udp receive admission
3. close active connections after flush
4. drain business tasks until timeout
5. stop logger admission
6. drain logger queue
7. flush and shutdown log backends
8. stop service tasks
9. stop executors and join threads
10. release pools/slabs
```

running task 不强杀；pending task、timer task 和尚未执行的控制 task 可以在 stop 策略下取消并释放生命周期引用。网络连接优先 `close_after_flush()`，超过 `connection_close_timeout` 后强制 close。

## 性能原则

- 任务投递统一 intrusive MPSC，减少多路径顺序语义差异。
- task inbox unbounded，避免非首次调度遇到队列满后需要复杂恢复。
- 日志 bounded MPSC，背压点放在日志入口，不让 log record pool 伪装成容量限制器。
- 对象池快路径使用 local cache，跨线程释放批量回收。
- 热路径结构按 cache line 对齐，避免 producer/consumer cursor、统计计数器和状态位 false sharing。
- reactor 默认 LT 模式，只在 fd 注册、interest 变化、取消和关闭时触发内核修改。
- TCP 读写按预算 drain，避免单连接饿死同 reactor 上的 task/timer/service。
- handler 按 listener/shard 拷贝，避免多个 IO 线程共享 handler 状态。
- hot path 优先数组、vector、slot table 和 generation；冷控制面可使用 `absl::flat_hash_map`。
- 网络输入尽量使用 `buffer_view` 零拷贝解析；输出优先 move buffer 和 scatter/gather。`af::buffer`
  直接以 Folly `IOBuf` 作为底层存储，依赖其 headroom/tailroom、clone/unshare 和 chain 语义继续推进零拷贝 IO。
- 分支预测上把成功路径、非错误路径和常见 readiness 路径作为直线代码，错误、关闭、溢出走冷函数。

## 目录布局

目标目录按模块和职责分层：

```text
include/af/
  runtime/
    runtime.hpp
    config_types.hpp
    config_resolution.hpp
    task.hpp
    work.hpp
    parallel.hpp
    detail/
      executor.hpp
      executor_loop.hpp
      task_pool.hpp
  queue/
    intrusive_mpsc_queue.hpp
    bounded_mpsc_queue.hpp
    bounded_mpmc_queue.hpp
    bounded_queue_common.hpp
    bounded_queues.hpp
    queue_backoff.hpp
  memory/
    slab_pool.hpp
    cache_line.hpp
  timer/
    timer_backend.hpp
    timer_entry.hpp
    timer_heap.hpp
    hierarchical_timer_wheel.hpp
  reactor/
    reactor.hpp
    fd_event_source.hpp
    detail/
      epoll_reactor.hpp
      kqueue_reactor.hpp
      select_reactor.hpp
  net/
    endpoint.hpp
    tcp/
      tcp_server.hpp
      tcp_listener.hpp
      tcp_connection.hpp
      tcp_client.hpp
    udp/
      udp_socket.hpp
    unix/
      unix_stream_server.hpp
      unix_stream_client.hpp
      unix_datagram_socket.hpp
  log/
    logger.hpp
    log_record.hpp
    log_backend.hpp
    file_backend.hpp
    udp_backend.hpp
    tcp_backend.hpp
  platform/
    hardware_threads.hpp
    thread_name.hpp
    affinity.hpp
    futex.hpp
```

原则：公开 API 和 detail 实现分开；reactor、tcp、udp、unix、log、runtime 不挤在一个大文件里；跨模块只依赖稳定抽象。

## 迁移顺序

1. 清理 `io_uring` 相关代码、测试、benchmark 和兼容桩。
2. 固化 C++17、conan 依赖和现代 CMake target。
3. 收敛命名、配置结构和 thread layout，只保留 `io/cpu`。
4. 抽出 queue、memory、timer、reactor 基础模块。
5. 用实例 runtime 路径承载 task、timer、service task 和 logger。
6. 重建 TCP server/client、UDP socket、Unix socket，所有 fd 操作 reactor-affine。
7. 更新 tcp echo server 和 tcp login server 示例；login 示例使用 protobuf。
8. 删除旧接口、兼容层和历史示例/测试。
9. 补 correctness stress、TSAN 可运行测试、benchmark 和 perf 分析脚本。
10. 在远端 Linux 容器中做 GCC/Clang 构建、ctest、benchmark、perf 回归。

## 测试与性能验证

正确性测试：

- task 跨线程调度、same-thread schedule、timer 到期/取消、shutdown race。
- MPSC 多生产者压力、对象池跨线程释放、OOM 策略。
- epoll/kqueue/select backend readiness、wake、close、interest 变化。
- TCP 多 listener、运行中 add/remove listener、reuse_port、多 IO 线程 accept。
- TCP handle generation、close_after_flush、输出高水位和跨线程 send。
- UDP 多 shard recv/send、connected UDP、send_to、stop race。
- logger ordered/relaxed 顺序、drop counter、flush timeout、backend shutdown。

性能验证：

- task post/hop ops/s。
- timer schedule/cancel/expire throughput。
- TCP echo 吞吐、延迟和 CPU 利用率。
- UDP datagram 吞吐和丢包策略。
- logger 单生产者、多生产者、ordered merge 和 backend batch 吞吐。
- perf 观察 cache-misses、branch-misses、cycles、context-switches、syscalls。

远端 Linux 验证应在 `/data` 下使用缓存构建，容器需支持 perf、epoll 和必要内核能力。
