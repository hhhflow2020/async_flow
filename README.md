# AsyncFlow

一个基于固定线程枚举的轻量 C++20 异步任务框架。

## 线程定义

业务使用连续递增 enum 定义线程，建议用 signed underlying type，并把第一个哨兵定义为 `enum_thread_index_start = -1`，最后一个哨兵定义为 `enum_thread_index_end`：

```cpp
enum class AppThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    IO_0,
    enum_thread_index_end,
};

struct AppRuntimeTraits {
    using Thread = AppThread;
    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AppThread::enum_thread_index_end);

    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(AppThread thread) noexcept {
        return thread == AppThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
```

`AppRuntimeTraits` 只建议放框架直接消费的参数：线程 enum、线程总数、队列容量、满队列策略等。业务分片范围放到具体逻辑里计算：

```cpp
inline constexpr AppThread player_logic_begin = AppThread::Logic_0;
inline constexpr std::uint16_t player_logic_shard_count =
    static_cast<std::uint16_t>(
        async::thread_index(AppThread::Logic_3) - async::thread_index(player_logic_begin) + 1U);

inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return async::shard_by<player_logic_begin, player_logic_shard_count>(player_id);
}
```

## 任务 API

业务任务继承 `async::Task`，通过 `schedule()` 首次调度，通过 `pending_on()` 切线程挂起，通过 `again()` 在当前线程继续，通过 `failed()` 失败结束并释放任务。`schedule()` 和 `async::make_task()` 之后调用的启动函数都能表达入队是否成功，队列满时不会无限增长。

```cpp
class LoginTask final : public Task {
public:
    explicit LoginTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id) {
        player_id_ = player_id;
        return schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            return start();
        case State::QueryDb:
            return query_db();
        case State::BackToLogic:
            return back_to_logic();
        case State::Finish:
            return finish();
        }
        return done();
    }

    af::TaskResult start() {
        state_ = State::QueryDb;
        return pending_on(AppThread::DB_0);
    }

    af::TaskResult query_db() {
        state_ = State::BackToLogic;
        return pending_on(player_thread(player_id_));
    }

    af::TaskResult back_to_logic() {
        state_ = State::Finish;
        return again();
    }

    af::TaskResult finish() {
        return done();
    }

    enum class State { Start, QueryDb, BackToLogic, Finish };
    State state_{State::Start};
    std::uint64_t player_id_{0};
};
```

每个任务构造函数必须把 `Task::FactoryToken` 作为第一个参数并传给基类。这个 token 只有 runtime 能生成，因此业务代码无法直接在栈上构造任务，也无法直接 `new` 任务。业务侧也不要直接 `delete` 任务对象。

推荐的启动方式是两步式：先通过框架工厂创建受管任务对象，再调用任务自己的启动函数；这个函数可以叫 `do_it()`，也可以是业务自定义名称，只要在函数里完成首次 `schedule()`：

```cpp
auto task = async::make_task<LoginTask>();
const bool started = task->do_it(player_id);
if (!started) {
    // 未调度成功时，task handle 析构会自动回收到对象池。
}
```

`make_task<T>()` 返回的是一个轻量包装句柄，内部由 runtime 统一接入对象池、生命周期引用和后续可替换的内存池策略。`async::start_task<T>()` 作为便捷入口保留，但公开示例统一使用 `make_task<T>() + do_it()`。

## IO 线程

traits 可通过 `thread_kind()` 把固定线程声明为 IO 线程。Linux 上 `ThreadKind::Epoll` 会在该 executor 内创建 epoll + eventfd，其他线程投递任务时通过合并唤醒写 eventfd；IO 线程空闲时直接阻塞在 `epoll_wait()`，任务和 fd readiness 都在同一个固定线程恢复。`ThreadKind::IoUring` 会优先创建 io_uring，并保留 epoll + eventfd 作为 completion wake 和 socket readiness fallback；如果内核或容器环境不支持 io_uring，该线程仍可退化为 epoll readiness 线程。

```cpp
static constexpr af::ThreadKind thread_kind(AppThread thread) noexcept {
    return thread == AppThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
}
```

任务必须先调度到对应 IO 线程，再调用 `wait_io()` 注册 fd 事件；事件到达后 runtime 会把 pending task 放回同一个 IO executor：

```cpp
state_ = State::Consume;
if (!wait_io(AppThread::IO_0, fd_, af::io_readable, &result_)) {
    return failed();
}
return pending();
```

业务侧更推荐使用 `af::io_accept_some()` / `af::io_connect()` / `af::io_read_some()` / `af::io_write_some()` / `af::io_recv_some()` / `af::io_send_some()` / `af::io_recv_from_some()` / `af::io_send_to_some()` 这组 helper。需要减少协议拼包拷贝时，可使用 `readv/writev`、`recvv/sendv`、`recvv_from/sendv_to` 和 `readv_at/writev_at` 这组 scatter/gather helper。普通 epoll 线程会先尝试一次非阻塞 syscall；遇到 `EAGAIN` / `EWOULDBLOCK` 时注册对应 readiness，并返回 `IoStep::Pending`。`ThreadKind::IoUring` 线程上，TCP `accept/connect/recv/send` 会优先提交 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT` / `IORING_OP_RECV` / `IORING_OP_SEND`，stream/datagram vectored IO 会优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，文件 vectored IO 会优先提交 `IORING_OP_READV` / `IORING_OP_WRITEV`，UDP `recv_from_some()` / `send_to_some()` 会优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`；当 backend 不可用、ring 满或 socket completion 返回 would-block 时退回 epoll readiness：

```cpp
const af::IoStatus status = af::io_read_some(
    *this,
    AppThread::IO_0,
    fd_,
    &value_,
    sizeof(value_),
    read_);
if (status.pending()) {
    return pending();
}
if (!status.ready()) {
    return failed();
}
```

业务模板代码可以进一步用轻量 adapter 包住 `thread + fd`，避免每个状态都重复传线程和 fd：

```cpp
af::TcpStream<AppThread> stream(AppThread::IO_0, fd_);
const af::IoStatus status = stream.recv_some(*this, buffer_, sizeof(buffer_), read_);
```

`af::IoFile<Thread>` 面向非阻塞 fd/readiness，并提供 `readv_at/writev_at` 文件 scatter/gather；`af::TcpListener<Thread>` 使用 `accept`，`af::TcpStream<Thread>` 使用 `connect/recv/send/recvv/sendv`，`af::UdpSocket<Thread>` 使用 `recvmsg/sendmsg`、`recvv_from/sendv_to` 或 `recvfrom/sendto` fallback，`af::IoEvent<Thread>` 使用 Linux `eventfd` readiness，`af::IoTimer<Thread>` 使用 Linux `timerfd` readiness。adapter 本身都是两个字段的小对象，不拥有 fd、不分配内存、不跨线程搬运 IO；任务仍然先调度到绑定的 IO 线程，syscall、io_uring submit/completion、eventfd/timerfd readiness 和后续恢复都在同一个 executor 上完成。需要所有权时可使用 `af::UniqueFd` 在业务侧管理 fd 生命周期。

在 `ThreadKind::IoUring` 线程上，`af::IoFile` 还提供 `read_at()` / `write_at()` / `readv_at()` / `writev_at()` / `fsync()`，通过 io_uring 提交真正的文件异步操作；`af::TcpListener`、`af::TcpStream` 和 `af::UdpSocket` 也会优先走 io_uring。业务可通过 `async::io_uring_backend_available(thread)` 判断是否启用。非 Linux 平台会保留接口但不创建 IO backend，业务可通过 `async::io_backend_available(thread)` 做降级判断。`iovec` 数组和 buffer 必须存活到 pending IO 恢复，建议作为 task 成员保存。`examples/io_epoll.cpp` 演示 socketpair readiness，`examples/io_event.cpp` 演示 eventfd 异步通知，`examples/io_timer.cpp` 演示 timerfd 异步定时器，`examples/io_adapters.cpp` 演示 TCP/UDP adapter，`examples/io_uring_file.cpp` 演示文件 write/fsync/read，`examples/io_uring_datagram.cpp` 演示 UDP client/server 全异步 round trip，`examples/io_tcp_connect_accept.cpp` 演示 TCP accept/connect/send/recv round trip，`examples/io_vectored.cpp` 演示 stream scatter/gather round trip。

## 批处理 API

普通批处理可以显式给出 shard 起点，也可以使用默认从线程 0 开始的便捷重载：

```cpp
async::parallel_shards(sharded_ops, af::ParallelMode::NonEmptyOnly, this, handler);
```

有序 batch 会强制走 `AllShards` 语义，并推进每个 shard 的 `last_applied_batch_id`：

```cpp
async::parallel_shards(sharded_ops, af::ParallelMode::AllShards, batch_id, this, handler);
```

框架也提供轻量 CRUD batch 类型，业务可以直接组合成自己的有序变更流：

```cpp
af::ChangeBatch<std::uint64_t, PlayerDelta> batch;
auto sharded = af::split_change_batch(batch, player_logic_shard_count);
```

## 性能边界

- runtime 固定线程之间使用 bounded SPSC ring，每个 source -> target 一条队列。
- runtime 线程调度到自身时走 executor 本地 bounded queue，阻塞路径会先消化本地任务，避免固定容量队列满时自旋等待自己。
- 非 runtime 线程进入 executor 时使用 bounded MPSC ingress，用于 `start_task()` 等外部入口。
- 队列容量由 traits 配置；`QueueFullPolicy::Reject` 直接返回失败，`QueueFullPolicy::Yield` 会让出 CPU 等待空位。
- shutdown 会先切到 stopping 并等待在途外部 post 退出，再停止 executor，避免队列清理和外部投递并发踩踏。
- `ShutdownPolicy::WaitForTasks` 会让 `shutdown()` 等已接收任务全部结束；`ShutdownPolicy::StopImmediately` 不等待未完成任务，traits 可通过 `enable_task_registry = true` 开启任务注册表，在 shutdown 后取消并释放仍处于 `Pending` / `Queued` 的任务。
- `make_task<T>()` / `start_task<T>()` 使用按任务类型分离的对象池，slot 回收通过 per-block bounded MPMC free queue 避免 Treiber free-list ABA；`create_task<T>()` 作为兼容别名保留。
- executor 空闲等待使用 C++20 `std::atomic::wait/notify_one`。
- Task 生命周期由状态机保护，debug 下会检查重复调度、完成后调度、运行中重复唤醒。
- `parallel_shards_ordered()` 会对每个 shard 维护 `last_applied_batch_id`，要求 batch id 连续递增。
- `parallel_shards_ordered(..., af::retryable_ordered_batch_options, ...)` 支持重试同一个 batch 时跳过已经应用成功的 shard；`af::OrderedBatchRetrySkipPolicy` 可用于业务侧记录失败次数并决定重试、跳过或停止。
- `start_ordered_task<Stream, ApplyTask>()` 会在指定 sequencer 线程上缓存乱序 batch，并按 batch_id 连续启动 apply task；如果 apply task 启动失败，不推进期待 batch id，后续重试仍从失败 batch 开始。
- Linux IO executor 使用 epoll + eventfd，`wait_io()` 注册一次性 fd readiness 后恢复原 pending task；跨线程唤醒做合并写，避免每次任务投递都写 eventfd。
- `af::io_read_some()` / `af::io_write_some()` / `af::io_recv_some()` / `af::io_send_some()` / `af::io_recv_from_some()` / `af::io_send_to_some()` 封装了非阻塞 fd 的 EAGAIN -> wait -> resume 流程，减少业务任务里重复写 syscall 分支；`readv/writev`、`recvv/sendv`、`recvv_from/sendv_to` 和 `readv_at/writev_at` 支持 scatter/gather，减少协议 framing、日志聚合等场景的中间拷贝。
- `af::make_eventfd()` / `af::write_eventfd()` / `af::IoEvent::wait()` 封装 Linux eventfd，适合业务侧异步通知、轻量计数器和跨组件唤醒，event fd 仍然在绑定 IO 线程上恢复 task。
- `af::make_timerfd()` / `af::arm_timerfd_after()` / `af::arm_timerfd_every()` / `af::IoTimer::wait()` 封装 Linux timerfd，适合超时、重试、心跳和连接保活，计时 fd 仍然在绑定 IO 线程上恢复 task。
- `ThreadKind::IoUring` 在同一个 IO executor 内用 raw io_uring syscall 提交 `read_at` / `write_at` / `readv_at` / `writev_at` / `fsync`、TCP `accept/connect/recv/send/recvv/sendv` 和 UDP `recvmsg/sendmsg/recvv_from/sendv_to`，completion 通过 eventfd 唤醒，不把 IO completion 跨线程搬运。
- `af::IoFile` / `af::TcpListener` / `af::TcpStream` / `af::UdpSocket` / `af::IoEvent` / `af::IoTimer` 是零堆分配 adapter，仅保存 `thread + fd` 并内联转发到 helper；它们不会引入额外队列或 MPMC hop。
- `CrudOp<Key, Value>` / `ChangeBatch<Key, Value>` 是纯数据 helper，不引入额外运行期状态。
- `parallel_shards()` 的 handler 如果返回 `bool`，`false` 会计为 shard 失败；owner 恢复后可用 `last_parallel_failures()` 读取失败数。
- `TaskResult::Cancelled` 可用于取消结束；runtime 未初始化或 stopping 时 `start_task()` 返回失败并销毁任务。
- 热路径不使用 `std::function`，shard handler 通过模板静态绑定。

## 构建与测试

```sh
conan install . --output-folder=build-conan --build=missing -s build_type=Release
cmake -S . -B build-conan/build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build-conan/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-conan/build/Release --parallel
ctest --test-dir build-conan/build/Release --output-on-failure
./build-conan/build/Release/asyncflow_runtime_benchmarks --benchmark_min_time=0.01s
./build-conan/build/Release/asyncflow_runtime_benchmarks \
  --benchmark_filter=BM_Runtime \
  --benchmark_min_time=0.01s \
  --benchmark_repetitions=7 \
  --benchmark_report_aggregates_only=true \
  --benchmark_out=runtime_benchmarks.json \
  --benchmark_out_format=json
python3 scripts/check_benchmark_regression.py runtime_benchmarks.json benchmarks/perf_baseline.json
```

TSAN/stress 可通过 CMake 选项打开：

```sh
conan install . --output-folder=build-tsan --build=missing -s build_type=Debug
cmake -S . -B build-tsan/build/Debug \
  -DCMAKE_TOOLCHAIN_FILE=build-tsan/build/Debug/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Debug \
  -DASYNCFLOW_ENABLE_TSAN=ON \
  -DASYNCFLOW_BUILD_BENCHMARKS=OFF
cmake --build build-tsan/build/Debug --parallel
ASYNCFLOW_STRESS_MS=1500 ctest --test-dir build-tsan/build/Debug -R RuntimeStressTests --output-on-failure
```

## 示例与测试布局

- `examples/app_runtime.hpp`：示例共享的线程 enum、traits、async alias 和分片函数。
- `examples/basic.cpp`：两步式 `make_task() + do_it()` 和状态机切线程。
- `examples/parallel_shards.cpp`：按 key 拆分 shard 并并行处理。
- `examples/ordered_batches.cpp`：乱序 batch 进入 sequencer，并用全 shard 顺序屏障应用。
- `examples/crud_apply.cpp`：完整 CRUD change batch 模板，包含乱序提交、sequencer、ordered shard apply。
- `examples/io_epoll.cpp`：Linux epoll IO 线程等待 fd readiness 并恢复 pending task。
- `examples/io_event.cpp`：使用 `af::IoEvent` 和 Linux eventfd 完成异步通知恢复。
- `examples/io_timer.cpp`：使用 `af::IoTimer` 和 Linux timerfd 完成异步定时器恢复。
- `examples/io_adapters.cpp`：使用 `af::TcpStream` 和 `af::UdpSocket` 编写业务状态机。
- `examples/io_uring_file.cpp`：使用 `af::IoFile::write_at()` / `fsync()` / `read_at()` 编写文件异步 IO 状态机。
- `examples/io_uring_datagram.cpp`：使用 `af::UdpSocket` 在 `ThreadKind::IoUring` 线程上完成 UDP client/server round trip。
- `examples/io_tcp_connect_accept.cpp`：使用 `af::TcpListener` 和 `af::TcpStream` 完成 TCP accept/connect/send/recv round trip。
- `examples/io_vectored.cpp`：使用 `af::TcpStream::sendv_some()` / `recvv_some()` 和 `af::UdpSocket::sendv_to_some()` / `recvv_from_some()` 完成 scatter/gather round trip。
- `tests/utility_tests.cpp`：队列、对象池、分片工具和 batch sequencer。
- `tests/runtime_lifecycle_tests.cpp`：任务生命周期、状态机、背压和 shutdown。
- `tests/runtime_io_tests.cpp`：IO 线程调度、epoll readiness 恢复、eventfd、timerfd、io_uring 文件、TCP accept/connect/stream、vectored stream/file/datagram 和 UDP datagram helper、read/write/TCP/UDP helper 与 adapter、重复 fd wait 拒绝、HUP/EOF、非法 fd、worker 误用降级，以及 StopImmediately 清理 pending IO wait。
- `tests/runtime_parallel_tests.cpp`：parallel shard、失败汇总、有序 batch 和 retryable ordered apply。
- `tests/runtime_stress_tests.cpp`：高并发 init/shutdown/start_task stress，CI 中也用于 TSAN job。
- `benchmarks/io_benchmarks.cpp`、`benchmarks/queue_benchmarks.cpp` 与 `benchmarks/runtime_benchmarks.cpp`：IO adapter、底层结构和 runtime 路径分开压测。
- `benchmarks/perf_baseline.json`：本地 runtime benchmark baseline。
- `benchmarks/perf_baseline_github_ubuntu.json` 与 `scripts/check_benchmark_regression.py`：GitHub Ubuntu runner 性能 baseline 与回归阈值检查。
