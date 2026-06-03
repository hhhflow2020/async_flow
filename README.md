# AsyncFlow

一个基于显式线程布局的轻量 C++20 异步任务框架。

## 线程定义

业务使用 tag + `thread_layout` 声明线程组，不再手写连续递增 enum。每个线程组声明自己的线程数量、线程能力和调试名称：

```cpp
struct AppLogicThreadTag;
struct AppDbThreadTag;
struct AppIoThreadTag;

struct AppRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<AppLogicThreadTag, 4, af::ThreadKind::Worker, "logic">(),
        af::thread_group<AppDbThreadTag, 1, af::ThreadKind::Worker, "db">(),
        af::thread_group<AppIoThreadTag, 1, af::ThreadKind::IoUring, "io">());

    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
    static constexpr unsigned io_uring_entries = 1024;
    static constexpr unsigned io_uring_cq_entries = 2048;
    static constexpr unsigned io_uring_submit_batch_threshold = 256;
    static constexpr bool io_uring_setup_single_issuer = true;
    static constexpr bool io_uring_setup_coop_taskrun = true;
    static constexpr bool io_uring_setup_defer_taskrun = true;
    static constexpr std::size_t io_wait_reserve = 1024;
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
using AppThread = async::Thread;

inline constexpr auto player_logic_threads = async::thread_group<AppLogicThreadTag>();
inline constexpr auto app_db_threads = async::thread_group<AppDbThreadTag>();
inline constexpr auto app_io_threads = async::thread_group<AppIoThreadTag>();

struct AppThreads {
    static constexpr AppThread Logic_0 = player_logic_threads.template at<0>();
    static constexpr AppThread Logic_1 = player_logic_threads.template at<1>();
    static constexpr AppThread Logic_2 = player_logic_threads.template at<2>();
    static constexpr AppThread Logic_3 = player_logic_threads.template at<3>();
    static constexpr AppThread DB_0 = app_db_threads.template at<0>();
    static constexpr AppThread IO_0 = app_io_threads.template at<0>();
};
```

`AppRuntimeTraits` 只建议放框架直接消费的参数：线程布局、队列容量、满队列策略、IO 后端容量等。`io_uring_entries` 必须是 2 的幂，`io_uring_submit_batch_threshold` 不能超过 entries；`io_uring_cq_entries` 为 0 时使用内核默认 CQ 大小，非 0 时会设置 `IORING_SETUP_CQSIZE`，且必须不小于 entries；`io_uring_setup_single_issuer`、`io_uring_setup_coop_taskrun`、`io_uring_setup_defer_taskrun`、`io_uring_setup_submit_all`、`io_uring_setup_sqpoll`、`io_uring_sqpoll_idle_ms` 和 `io_uring_sqpoll_cpu` 会映射到对应 `io_uring_setup(2)` flags/params，极致性能场景可按内核能力开启。`io_wait_reserve` 和 `io_uring_provided_buffer_group_reserve` 会在 IO executor 初始化时 best-effort 预留热点表空间，减少高并发 fd wait 和 provided-buffer group 注册时的 rehash/扩容抖动。

线程组对象是空的编译期视图，`begin/count/at()/shard()` 都是内联整数运算。任务切换时 `Thread` 本身只保存一个 `uint16_t` index，`pending_on()` / `post()` 直接使用这个 index 定位 executor 和 SPSC queue，不会因为通过 `thread_group` 计算目标线程而多一次对象跳转或堆内存访问。`thread_kind()` / `thread_name()` 这类 introspection 才会按 index 查询一张小的编译期表，主要用于 executor 初始化和调试，不在普通调度热路径里。

业务分片范围放到具体逻辑里计算：

```cpp
inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return player_logic_threads.shard(player_id);
}
```

`thread_group` 的第四个模板参数是线程组名称。Linux/macOS 上 runtime 启动 executor 时会调用系统线程命名接口，把线程命名为 `af-<group>-<offset>`，例如 `af-logic-0`、`af-io-0`，方便在 `top`、`ps -L`、`htop`、`lldb/gdb` 中定位线程；实际可见长度受平台线程名上限约束。

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
        return pending_on(AppThreads::DB_0);
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

线程组可在 `thread_group<Tag, Count, Kind, Name>()` 的第三个模板参数里声明 IO 能力。跨平台业务优先使用 `ThreadKind::Io`：Linux 下映射到 epoll readiness backend，macOS/BSD 下映射到 kqueue readiness backend，公共 `io_*` API 不变。Linux 上 `ThreadKind::Epoll` 可显式指定 epoll + eventfd；`ThreadKind::IoUring` 会优先创建 io_uring，并保留 epoll + eventfd 作为 completion wake 和 socket readiness fallback；如果内核或容器环境不支持 io_uring，该线程仍可退化为 epoll readiness 线程。

提示：某些容器运行时默认 seccomp/apparmor profile 可能拦截 `io_uring_setup/enter/register`，导致 `ThreadKind::IoUring` 退化为 epoll 并跳过 io_uring tests；在 OrbStack/Docker 下可尝试容器启动参数 `--security-opt seccomp=unconfined --security-opt apparmor=unconfined` 或使用自定义 seccomp profile 放通相关 syscall。

例如 `af::thread_group<AppIoThreadTag, 1, af::ThreadKind::Io, "io">()` 声明一个跨平台 native-readiness IO 线程；Linux 专用高性能路径可改成 `ThreadKind::IoUring`。

任务必须先调度到对应 IO 线程，再调用 `wait_io()` 注册 fd 事件；事件到达后 runtime 会把 pending task 放回同一个 IO executor：

```cpp
state_ = State::Consume;
if (!wait_io(AppThreads::IO_0, fd_, af::io_readable, &result_)) {
    return failed();
}
return pending();
```

业务侧更推荐使用 `af::io_socket()` / `af::io_bind()` / `af::io_listen()` / `af::io_setsockopt()` / `af::io_getsockopt()` / `af::io_getsockname()` / `af::io_getpeername()` / `af::io_accept_some()` / `af::io_connect()` / `af::io_shutdown()` / `af::io_read_some()` / `af::io_write_some()` / `af::io_recv_some()` / `af::io_send_some()` / `af::io_send_zc_some()` / `af::io_recv_from_some()` / `af::io_send_to_some()` 这组 helper。文件生命周期可用 `af::io_openat()` / `af::io_openat2()` / `af::io_mkdirat()` / `af::io_ftruncate()` / `af::io_statx()` / `af::io_linkat()` / `af::io_symlinkat()` / `af::io_renameat()` / `af::io_unlinkat()` / `af::io_close()`，目录归属清晰时也可用 `af::IoDirectory<Thread>` 薄 adapter。需要减少协议拼包拷贝时，可使用 `readv/writev`、`recvv/sendv`、`recvv_from/sendv_to` 和 `readv_at/writev_at` 这组 scatter/gather helper；需要减少文件 buffer pin/unpin 时，可先在 IO 线程调用 `async::io_register_buffers()`，再用 `read_fixed_at/write_fixed_at`；需要减少 socket 接收侧重复提交和业务 buffer 分发时，可用 `af::IoProvidedBufferRing` + `async::io_register_provided_buffer_ring()` + TCP/连接型 UDP 的 `recv_multishot()`，或非连接 UDP 的 `recv_from_multishot()`；需要减少 fd table lookup 时，可先调用 `async::io_register_files()`，用 `async::io_update_registered_files()` 热替换 slot，再用 `af::IoFixedFile` 的 `read_at/write_at/readv_at/writev_at`；如果希望 open/accept 结果直接进入 registered file slot，可使用 `af::io_openat_direct()` / `af::io_accept_direct()`，避免普通 fd 安装后再 update slot；需要高连接建立吞吐时，可在 io_uring 线程使用实验性 `accept_multishot()`；需要内核态搬运时，可使用 `af::io_sendfile_some()` / `af::io_splice_some()`；需要发送侧减少用户态拷贝和 ACK 前 buffer 生命周期阻塞时，可使用 `af::io_send_zc_some()`、TCP `sendv_zc_some()` 或 UDP `send_zc_to_some()` / `sendv_zc_to_some()`。普通 native readiness 线程会先尝试一次非阻塞 syscall；遇到 `EAGAIN` / `EWOULDBLOCK` 时注册对应 readiness，并返回 `IoStep::Pending`。`ThreadKind::IoUring` 线程上，`io_socket()`、`io_openat()`、`io_openat2()`、`io_openat_direct()`、`io_accept_direct()`、`io_close()`、`io_statx()`、`io_fallocate()`、`io_ftruncate()`、`io_renameat()`、`io_unlinkat()`、`io_mkdirat()`、`io_linkat()`、`io_symlinkat()`、`io_read_some()`、`io_write_some()`、`io_readv_some()`、`io_writev_some()`、`io_splice_some()`、TCP `accept/accept_direct/connect/shutdown/recv/send/send_zc/recvv/sendv/sendv_zc/recv_multishot`、连接型 UDP `recv_multishot` 和非连接 UDP `recv_from_multishot` 会优先提交 `IORING_OP_SOCKET` / `IORING_OP_OPENAT` / `IORING_OP_OPENAT2` / `IORING_OP_CLOSE` / `IORING_OP_STATX` / `IORING_OP_FALLOCATE` / `IORING_OP_FTRUNCATE` / `IORING_OP_RENAMEAT` / `IORING_OP_UNLINKAT` / `IORING_OP_MKDIRAT` / `IORING_OP_LINKAT` / `IORING_OP_SYMLINKAT` / `IORING_OP_READ` / `IORING_OP_WRITE` / `IORING_OP_READV` / `IORING_OP_WRITEV` / `IORING_OP_SPLICE` / `IORING_OP_ACCEPT` / `IORING_OP_CONNECT` / `IORING_OP_SHUTDOWN` / `IORING_OP_RECV` / `IORING_OP_SEND` / `IORING_OP_SEND_ZC` / `IORING_OP_SENDMSG_ZC`，direct open/accept 会设置 SQE `file_index` 把结果落到目标 registered file slot，`bind/listen/setsockopt/getsockopt/getsockname/getpeername` 当前在目标 IO 线程执行轻量 syscall，等内核和头文件暴露对应 io_uring opcode 后再接入 ring；`accept_multishot()` 会在 `IORING_OP_ACCEPT` 上设置 `IORING_ACCEPT_MULTISHOT`，`recv_multishot()` 会在 `IORING_OP_RECV` 上设置 `IORING_RECV_MULTISHOT | IOSQE_BUFFER_SELECT`，`recv_from_multishot()` 会在 `IORING_OP_RECVMSG` 上设置 `IORING_RECV_MULTISHOT | IOSQE_BUFFER_SELECT`，stream/datagram vectored IO 会优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，固定 buffer 文件 IO 会提交 `IORING_OP_READ_FIXED` / `IORING_OP_WRITE_FIXED`，fixed file 文件/stream 标量和 scatter/gather IO 会设置 `IOSQE_FIXED_FILE`，文件 vectored IO 会优先提交 `IORING_OP_READV` / `IORING_OP_WRITEV`，UDP `recv_from_some()` / `send_to_some()` 会优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，UDP zero-copy send helper 会优先提交 `IORING_OP_SENDMSG_ZC`；当 socket completion 返回 would-block 或 sendfile 这类 syscall fallback 需要 readiness 时，若 `IORING_OP_POLL_ADD` 可用会继续留在 ring 内等待，否则退回 epoll readiness；当 backend 不可用、ring 满、`IORING_OP_SOCKET` 不可用或 `SEND_ZC` / `SENDMSG_ZC` 不被内核/socket 支持时退回目标 IO 线程上的 syscall/readiness：

```cpp
const af::IoStatus status = af::io_read_some(
    *this,
    AppThreads::IO_0,
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
af::TcpStream<AppThread> stream(AppThreads::IO_0, fd_);
const af::IoStatus status = stream.recv_some(*this, buffer_, sizeof(buffer_), read_);
```

`af::IoFile<Thread>` 面向非阻塞 fd/readiness，并提供 `readv_at/writev_at` 文件 scatter/gather；`af::IoDirectory<Thread>` 面向目录 fd，只保存 `thread + fd` 并内联转发 `openat2/mkdirat/linkat/symlinkat`；`af::IoFixedFile<Thread>` 面向 io_uring registered file table，只保存 `thread + file_index`，可用于 fixed-file 文件 `read_at/write_at/readv_at/writev_at`，也可对 direct-accepted socket 调用 `recv_some/send_some/recvv_some/sendv_some`；descriptor adapter 通用提供 `setsockopt/getsockopt/getsockname/getpeername` endpoint 查询；`af::TcpListener<Thread>` 使用 `bind/listen/accept/accept_direct/accept_multishot`，`af::TcpStream<Thread>` 使用 `connect/shutdown/recv/send/send_zc/sendv_zc/recvv/sendv/sendfile_some`，`af::UdpSocket<Thread>` 使用 `bind`、`recvmsg/sendmsg`、`send_zc_to/sendv_zc_to`、`recvv_from/sendv_to`、连接型 `recv_multishot`、非连接型 `recv_from_multishot` 或 `recvfrom/sendto` fallback，`af::IoEvent<Thread>` 使用 Linux `eventfd` readiness，`af::IoTimer<Thread>` 使用 Linux `timerfd` readiness。adapter 本身都是两个字段的小对象，不拥有 fd、不分配内存、不跨线程搬运 IO；任务仍然先调度到绑定的 IO 线程，syscall、io_uring submit/completion、eventfd/timerfd readiness 和后续恢复都在同一个 executor 上完成。需要所有权时可使用 `af::UniqueFd` 在业务侧管理 fd 生命周期。

已经挂起的 IO 可在对应 IO 线程调用 `async::cancel_io(thread, state)` 取消；epoll readiness 会从 epoll 删除 fd wait，io_uring completion 会提交 `IORING_OP_ASYNC_CANCEL`，最终都用 `ECANCELED` 恢复原 pending task。为了避免跨线程改动 `IoOpState` 和 executor 内部 wait 表，业务侧应把取消动作也调度到 fd 所属 IO 线程执行；`io_close()` 提交成功后 fd 所有权已释放，因此不支持再取消 pending close。单次 IO 超时可使用 `af::IoDeadline` + `af::arm_io_timeout()` 组合：io_uring 线程优先用 `IORING_OP_TIMEOUT` 留在 ring 内等待，macOS/BSD `ThreadKind::Io` 使用 kqueue one-shot timer，Linux epoll/fallback 使用 timerfd；IO 先完成会取消 timeout wait，timeout 先完成会取消 pending IO 并返回 `ETIMEDOUT`，用户 cancel 仍返回 `ECANCELED`。只需要纯定时等待时，可在 io_uring 或 kqueue IO 线程直接使用 `af::io_wait_timeout()`，避免额外 timerfd。

在 `ThreadKind::IoUring` 线程上，`af::io_openat()`、`af::io_openat2()`、`af::io_openat_direct()`、`af::io_accept_direct()`、`af::io_close()`、`af::io_statx()`、`af::io_fallocate()`、`af::io_ftruncate()`、`af::io_mkdirat()`、`af::io_linkat()`、`af::io_symlinkat()`、`af::io_renameat()`、`af::io_unlinkat()`、`af::io_splice_some()` 和 `af::IoFile` 的 `read_some()` / `write_some()` / `readv_some()` / `writev_some()` / `read_at()` / `write_at()` / `read_fixed_at()` / `write_fixed_at()` / `readv_at()` / `writev_at()` / `fsync()` 通过 io_uring 提交真正的文件异步操作；不带 offset 的 `*_some()` 会使用并推进 fd 当前文件偏移。新增 filesystem helper 走专用 SQE 快路径，避开 message/address/multishot/fixed-file 等无关分支，并复用 runtime 的 io_uring operation 对象池。`af::IoFixedFile` 的 `read_at()` / `write_at()` / `readv_at()` / `writev_at()` / `fsync()` 使用 registered file index 并设置 `IOSQE_FIXED_FILE`，`read_fixed_at()` / `write_fixed_at()` 会同时使用 registered file table 和 registered buffer table，`recv_some()` / `send_some()` / `recvv_some()` / `sendv_some()` 可驱动 direct descriptor socket；`async::io_update_registered_files()` 使用 `IORING_REGISTER_FILES_UPDATE` 替换已注册 file slot，要求同一 IO 线程且无 pending fixed-file op；`af::io_openat_direct()` / `af::io_accept_direct()` 要求先注册 sparse file table，并让 open/accept CQE 直接填充指定 slot。`af::TcpListener`、`af::TcpStream` 和 `af::UdpSocket` 也会优先走 io_uring，`af::TcpListener::accept_multishot()` 会保留同一个 SQE 持续产出 accepted fd，`af::TcpStream::recv_multishot()` 和连接型 `af::UdpSocket::recv_multishot()` 会从 registered provided buffer ring 选择 buffer 并在 `IoResult` 中返回 buffer id；非连接型 `af::UdpSocket::recv_from_multishot()` 通过 `IORING_OP_RECVMSG` 返回 selected buffer id，业务用 `af::io_parse_recvmsg_multishot_buffer()` 从同一个 buffer 中解析 peer address/control/payload offset，避免把地址或 payload 再拷贝到框架临时缓冲。业务消费后把该 buffer 重新 `add()` 回 ring；业务完成一批接收后应在同一 IO 线程调用 `async::cancel_io()` 停止 multishot op。为避免 burst 场景复用地址缓冲导致 fd 与 peer address 不对应，multishot accept 当前只返回 fd，不填充 peer address。业务可通过 `async::io_uring_backend_available(thread)` 判断是否启用，通过 `async::io_uring_poll_available(thread)` 判断 readiness fallback 是否也会使用 `IORING_OP_POLL_ADD`。固定 buffer 必须先在对应 IO 线程用 `async::io_register_buffers(thread, iov, count)` 注册，并在无 pending fixed IO 后用 `async::io_unregister_buffers(thread)` 释放；provided buffer ring 必须先用 `af::IoProvidedBufferRing::init()` 分配 power-of-two ring、`add()` 填入业务 buffer，并在 IO 线程用 `async::io_register_provided_buffer_ring()` 注册，停止 pending multishot op 后再 `async::io_unregister_provided_buffer_ring()`；fixed file 必须先用 `async::io_register_files(thread, fds, count)` 注册，可用 `async::io_update_registered_files(thread, offset, fds, count)` 热替换 slot，并在无 pending fixed file IO 后用 `async::io_unregister_files(thread)` 释放。Linux epoll 和 macOS/BSD kqueue 都会创建 native readiness backend；其他平台会保留接口但不创建 IO backend，业务可通过 `async::io_backend_available(thread)` 做降级判断。`iovec` 数组、buffer、路径字符串和 sendfile/splice offset 指针必须存活到 pending IO 恢复，建议作为 task 成员保存。`examples/io_native_readiness.cpp` 演示跨平台 native readiness，`examples/io_epoll.cpp` 演示 Linux epoll socketpair readiness，`examples/io_event.cpp` 演示 eventfd 异步通知，`examples/io_timer.cpp` 演示 timerfd 异步定时器，`examples/io_adapters.cpp` 演示 TCP/UDP adapter，`examples/io_sendfile_static.cpp` 演示文件经 TCP socket 少拷贝发送，`examples/io_uring_file.cpp` 演示当前偏移文件 write/fsync/read，`examples/io_uring_fixed_buffer.cpp` 演示 registered buffer + fixed read/write，`examples/io_uring_fixed_file.cpp` 演示 registered file table + slot update + registered buffer + fixed file `readv_at/writev_at`，`examples/io_uring_openat_direct.cpp` 演示 direct descriptor open 到 fixed file slot，`examples/io_uring_accept_direct.cpp` 演示 direct accept 到 fixed file slot 后用 `IoFixedFile::recvv_some()` / `sendv_some()` 做 scatter/gather round trip，`examples/io_uring_tuned_setup.cpp` 演示 CQ size、single issuer、cooperative/deferred taskrun 等 setup traits，`examples/io_uring_multishot_accept.cpp` 演示 multishot accept，`examples/io_uring_recv_multishot.cpp` 演示 TCP provided buffer ring + multishot recv，`examples/io_uring_udp_recv_multishot.cpp` 演示连接型 UDP multishot recv，`examples/io_uring_udp_recvmsg_multishot.cpp` 演示带 peer address 的非连接 UDP multishot recvmsg，`examples/io_uring_openat.cpp` 演示异步 openat + 文件 round trip，`examples/io_uring_file_lifecycle.cpp` 演示文件生命周期闭环，`examples/io_uring_filesystem_ops.cpp` 演示 openat2/mkdirat/ftruncate/linkat/symlinkat/unlinkat 组合，`examples/io_uring_datagram.cpp` 演示 UDP client/server 全异步 round trip，`examples/io_tcp_connect_accept.cpp` 演示 TCP accept/connect/send/recv round trip，`examples/io_vectored.cpp` 演示 stream scatter/gather round trip。

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
- 调度 API 提供 `Auto` / `Fast` / `Ordered` 三种语义：`Fast` 明确选择 runtime-thread-only 的 local/SPSC 快速路径，`Ordered` 明确强制走目标 MPSC；即使 runtime 线程投递给自身目标，也可以通过 `schedule_ordered()` / `pending_ordered()` 选择 MPSC 顺序路径。完整说明见 [Runtime 调度语义](docs/runtime_scheduling_semantics.md)。
- 队列容量由 traits 配置；`QueueFullPolicy::Reject` 直接返回失败，`QueueFullPolicy::Yield` 会让出 CPU 等待空位。
- shutdown 会先切到 stopping 并等待在途外部 post 退出，再停止 executor，避免队列清理和外部投递并发踩踏。
- `ShutdownPolicy::WaitForTasks` 会让 `shutdown()` 等已接收任务全部结束；`ShutdownPolicy::StopImmediately` 不等待未完成任务，traits 可通过 `enable_task_registry = true` 开启任务注册表，在 shutdown 后取消并释放仍处于 `Pending` / `Queued` 的任务。
- `make_task<T>()` / `start_task<T>()` 使用按任务类型分离的对象池，slot 回收通过 per-block bounded MPMC free queue 避免 Treiber free-list ABA；`create_task<T>()` 作为兼容别名保留。
- executor 空闲等待使用 C++20 `std::atomic::wait/notify_one`。
- Task 生命周期由状态机保护，debug 下会检查重复调度、完成后调度、运行中重复唤醒。
- `parallel_shards_ordered()` 会对每个 shard 维护 `last_applied_batch_id`，要求 batch id 连续递增。
- `parallel_shards_ordered(..., af::retryable_ordered_batch_options, ...)` 支持重试同一个 batch 时跳过已经应用成功的 shard；`af::OrderedBatchRetrySkipPolicy` 可用于业务侧记录失败次数并决定重试、跳过或停止。
- `start_ordered_task<Stream, ApplyTask>()` 会在指定 sequencer 线程上缓存乱序 batch，并按 batch_id 连续启动 apply task；如果 apply task 启动失败，不推进期待 batch id，后续重试仍从失败 batch 开始。
- Linux IO executor 使用 epoll + eventfd，`wait_io()` 注册一次性 fd readiness 后恢复原 pending task；同一个 `IoOpState` 在同一 fd 上立即重挂时优先 `EPOLL_CTL_MOD`，readiness 完成后延迟清理内核注册，给恢复的 task 一次直接重挂机会，否则在阻塞前统一 `DEL`，减少热连接完成阶段 `DEL` 和下一轮 `ADD` 的系统调用；跨线程唤醒做合并写，避免每次任务投递都写 eventfd。
- `af::io_socket()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_SOCKET`，不支持时在目标 IO 线程执行 `socket(2)` fallback；`af::io_bind()` / `af::io_listen()` / `af::io_setsockopt()` / `af::io_getsockopt()` / `af::io_getsockname()` / `af::io_getpeername()` 把 socket setup 和 endpoint 查询也约束在 fd 所属 IO 线程。
- `af::io_openat()` / `af::io_openat2()` / `af::io_mkdirat()` / `af::io_close()` / `af::io_statx()` / `af::io_fallocate()` / `af::io_ftruncate()` / `af::io_linkat()` / `af::io_symlinkat()` / `af::io_renameat()` / `af::io_unlinkat()` 在 `ThreadKind::IoUring` 线程上提交文件生命周期操作，可把文件打开、目录创建、预分配、截断、元数据查询、link/symlink、rename/unlink 和关闭都放到 IO 线程。
- `af::io_read_some()` / `af::io_write_some()` / `af::io_recv_some()` / `af::io_send_some()` / `af::io_send_zc_some()` / `af::io_sendv_zc_some()` / `af::io_recv_from_some()` / `af::io_send_to_some()` / `af::io_send_zc_to_some()` / `af::io_sendv_zc_to_some()` 封装了非阻塞 fd 的 EAGAIN -> wait -> resume 流程，减少业务任务里重复写 syscall 分支；`io_send_zc_some()` 在 io_uring 线程优先使用 `IORING_OP_SEND_ZC`，`sendv_zc`/UDP zero-copy send helper 优先使用 `IORING_OP_SENDMSG_ZC`，并正确处理主 CQE 与 notification CQE 的两阶段完成；`readv/writev`、`recvv/sendv`、`recvv_from/sendv_to` 和 `readv_at/writev_at` 支持 scatter/gather，减少协议 framing、日志聚合等场景的中间拷贝。
- `af::io_sendfile_some()` 和 `af::io_splice_some()` 支持文件到 socket、fd 到 fd 的内核态搬运；`sendfile` 遇到 EAGAIN 等待 out fd writable，`splice` 在 io_uring 可用时优先 `IORING_OP_SPLICE`，fallback 会根据 fd readiness 选择等待输入 readable 或输出 writable。
- `af::make_eventfd()` / `af::write_eventfd()` / `af::IoEvent::wait()` 封装 Linux eventfd，适合业务侧异步通知、轻量计数器和跨组件唤醒，event fd 仍然在绑定 IO 线程上恢复 task。
- `af::make_timerfd()` / `af::arm_timerfd_after()` / `af::arm_timerfd_every()` / `af::IoTimer::wait()` 封装 Linux timerfd，适合超时、重试、心跳和连接保活，计时 fd 仍然在绑定 IO 线程上恢复 task；在 io_uring 线程上，纯一次性等待可用 `af::io_wait_timeout()` 提交 `IORING_OP_TIMEOUT`，不创建 timerfd、不进入 epoll wait 表。
- `async::cancel_io(thread, state)` 可取消同 IO 线程内的 epoll readiness pending wait 和 io_uring completion op，并用 `ECANCELED` 恢复原 task；正常 readiness helper 热路径只增加一个 `[[unlikely]]` 取消分支。
- `af::IoDeadline` / `af::arm_io_timeout()` 在 io_uring 线程优先使用 `IORING_OP_TIMEOUT`，在 macOS/BSD kqueue 线程使用 EVFILT_TIMER，在 Linux epoll 或 ring 不可用时复用绑定 IO 线程上的 timerfd，不引入跨线程 MPMC hop；同一个 task 的 IO wait 和 timeout wait 同时 ready 时 runtime 会合并重复唤醒，task 自己在下一次 `run()` 中消费结果。
- `ThreadKind::IoUring` 在同一个 IO executor 内用 raw io_uring syscall 提交当前偏移 `read/write/readv/writev`、指定 offset `read_at` / `write_at` / `read_fixed_at` / `write_fixed_at` / `readv_at` / `writev_at` / `fsync`、TCP `accept/accept_direct/connect/shutdown/recv/send/send_zc/sendv_zc/recvv/sendv`、连接型 UDP `recv_multishot`、非连接 UDP `recv_from_multishot`、UDP `recvmsg/sendmsg/sendmsg_zc/recvv_from/sendv_to/sendv_zc_to` 和 `timeout`，同一 executor tick 内的多个 SQE 会按 `io_uring_submit_batch_threshold` 合并提交以减少 `io_uring_enter`，默认 ring entries 为 1024；traits 可配置 CQ size、SQPOLL、SQ thread CPU affinity、`SUBMIT_ALL`、`COOP_TASKRUN`、`SINGLE_ISSUER`、`DEFER_TASKRUN` 和原始 setup flags，极致场景可减少 kernel task_work 触发和提交抖动；completion 通过 eventfd 唤醒，pending completion cancel 通过 `IoResult` 内的 token O(1) 定位 operation，不把 IO completion 跨线程搬运；readiness fallback 在内核支持时使用 `IORING_OP_POLL_ADD`，避免同一 IO 线程热路径同时维护 epoll wait 表和 ring op；普通 fd `read/write` 和 stream `recv/send` 使用分支更少的 buffer SQE submit 快路径，避开 path/message/address/multishot/direct/fixed-file 这些无关分类；固定 buffer 复用内核已注册的 buffer table，减少热文件 IO 的 pin/unpin 和页表处理开销；provided buffer ring + multishot recv 用一个 recv/recvmsg SQE 连续产出多个接收 CQE，并让内核从业务预填 ring 中选择 buffer，减少重复 submit、业务 buffer 分发分支和接收侧临时拷贝；fixed file 复用内核已注册的 file table，减少热路径 fd 查表和引用计数处理，`io_update_registered_files()` 可用 `IORING_REGISTER_FILES_UPDATE` 更新部分 slot，避免动态 fd 集合场景反复重建整张表；fixed-file 文件 `read/write/readv/writev/read_fixed/write_fixed` 使用分支更少的专用 SQE submit 快路径，避开 path/socket/message/multishot/zero-copy 这些无关 opcode 分类；`io_openat_direct()` / `io_accept_direct()` 进一步避免 normal fd 安装和 register update 这两个步骤；两者组合时同一个 SQE 同时使用 `IORING_OP_READ_FIXED/WRITE_FIXED` 与 `IOSQE_FIXED_FILE`；multishot accept 用一个 SQE 产出多个连接完成事件，降低高并发建连场景的 submit 次数。
- `af::IoFile` / `af::IoFixedFile` / `af::TcpListener` / `af::TcpStream` / `af::UdpSocket` / `af::IoEvent` / `af::IoTimer` 是零堆分配 adapter，仅保存 `thread + fd` 或 `thread + file_index` 并内联转发到 helper；它们不会引入额外队列或 MPMC hop。
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
- `examples/io_native_readiness.cpp`：使用 `ThreadKind::Io` 在 Linux epoll 或 macOS/BSD kqueue 上等待 fd readiness 并恢复 pending task。
- `examples/io_epoll.cpp`：Linux epoll IO 线程等待 fd readiness 并恢复 pending task。
- `examples/io_event.cpp`：使用 `af::IoEvent` 和 Linux eventfd 完成异步通知恢复。
- `examples/io_timer.cpp`：使用 `af::IoTimer` 和 Linux timerfd 完成异步定时器恢复。
- `examples/io_timeout.cpp`：使用 `af::IoDeadline` 为单次 pending read 组合 timeout/cancel，Linux epoll 使用 timerfd，macOS/BSD kqueue 使用 native timer。
- `examples/io_adapters.cpp`：使用 `af::TcpStream` 和 `af::UdpSocket` 编写业务状态机。
- `examples/io_pollable_client.cpp`：第三方 client 暴露 fd 的接入模板：`step()` 返回 WANT_READ/WANT_WRITE，由框架驱动 readiness 并恢复状态机。
- `examples/io_rpc_length_prefixed.cpp`：简单 length-prefixed RPC client/server：连接归属固定 IO 线程，消息处理切到逻辑线程，响应再切回 IO 线程发送。
- `examples/io_sendfile_static.cpp`：使用 `af::TcpStream::sendfile_some()` 将静态文件内容通过 TCP socket 发送给 peer。
- `examples/io_shutdown.cpp`：使用 `af::TcpStream::shutdown()` 在绑定 IO 线程完成 TCP half-close。
- `examples/io_socket_lifecycle.cpp`：在指定 IO 线程完成 TCP listener 的 socket、setsockopt、bind、listen 和 accept 生命周期。
- `examples/io_uring_send_zc.cpp`：在 io_uring IO 线程使用 `af::TcpStream::send_zc_some()` 发送 TCP payload；内核或 socket 不支持 `SEND_ZC` 时自动退回普通非阻塞 `send`。
- `examples/io_uring_sendmsg_zc.cpp`：在 io_uring IO 线程使用 `af::TcpStream::sendv_zc_some()` 发送 vectored payload；内核或 socket 不支持 `SENDMSG_ZC` 时自动退回普通非阻塞 `sendmsg`。
- `examples/io_uring_timeout.cpp`：使用 `af::io_wait_timeout()` 提交 ring-native timeout，不创建 timerfd。
- `examples/io_uring_file.cpp`：使用 `af::IoFile::write_at()` / `fsync()` / `read_at()` 编写文件异步 IO 状态机。
- `examples/io_uring_fixed_buffer.cpp`：使用 `async::io_register_buffers()` + `af::IoFile::write_fixed_at()` / `read_fixed_at()` 复用固定 buffer。
- `examples/io_uring_fixed_file.cpp`：使用 `async::io_register_files()` / `async::io_update_registered_files()` + `async::io_register_buffers()` + `af::IoFixedFile::write_fixed_at()` / `read_fixed_at()` / `writev_at()` / `readv_at()` 组合 fixed file table、fixed buffer table 与 scatter/gather 文件 IO。
- `examples/io_uring_openat_direct.cpp`：使用 sparse registered file table + `af::io_openat_direct()` 将 open 结果直接安装到 fixed file slot。
- `examples/io_uring_accept_direct.cpp`：使用 sparse registered file table + `af::TcpListener::accept_direct()` 将 accepted socket 直接安装到 fixed file slot，并用 `af::IoFixedFile::recvv_some()` / `sendv_some()` 做 scatter/gather round trip。
- `examples/io_uring_multishot_accept.cpp`：使用 `af::TcpListener::accept_multishot()` 通过单个 SQE 连续接收多个 TCP 连接。
- `examples/io_uring_recv_multishot.cpp`：使用 `af::IoProvidedBufferRing` + `af::TcpStream::recv_multishot()` 通过单个 SQE 连续接收 socket 数据，并按 CQE buffer id 归还 buffer。
- `examples/io_uring_udp_recv_multishot.cpp`：使用连接型 UDP socket + `af::UdpSocket::recv_multishot()` 通过 provided buffer ring 连续接收固定 peer UDP datagram。
- `examples/io_uring_udp_recvmsg_multishot.cpp`：使用非连接 UDP socket + `af::UdpSocket::recv_from_multishot()` 连续接收 datagram，并从 selected buffer 中解析 peer address 和 payload offset。
- `examples/io_uring_openat.cpp`：使用 `af::io_openat()` 异步创建文件，并继续 write/fsync/read。
- `examples/io_uring_file_lifecycle.cpp`：使用 `openat/fallocate/write/fsync/statx/rename/unlink/close` 完成文件生命周期闭环。
- `examples/io_uring_filesystem_ops.cpp`：使用 `openat2/mkdirat/write/ftruncate/fsync/statx/linkat/symlinkat/unlinkat` 完成目录和文件生命周期，并用 runtime idle 等待替代示例级完成标记 atomic。
- `examples/io_uring_datagram.cpp`：使用 `af::UdpSocket` 在 `ThreadKind::IoUring` 线程上完成 UDP client/server round trip。
- `examples/io_tcp_connect_accept.cpp`：使用 `af::TcpListener` 和 `af::TcpStream` 完成 TCP accept/connect/send/recv round trip。
- `examples/io_vectored.cpp`：使用 `af::TcpStream::sendv_some()` / `recvv_some()` 和 `af::UdpSocket::sendv_to_some()` / `recvv_from_some()` 完成 scatter/gather round trip。
- `tests/utility_tests.cpp`：队列、对象池、分片工具和 batch sequencer。
- `tests/runtime_lifecycle_tests.cpp`：任务生命周期、状态机、背压和 shutdown。
- `tests/runtime_io_*_tests.cpp`：按 setup、epoll、kqueue、stream/zero-copy、io_uring socket、io_uring file、datagram、shutdown 拆分 IO 覆盖；公共 fixture 和 task helper 放在 `tests/runtime_io_test_support.hpp`。
- `tests/runtime_parallel_tests.cpp`：parallel shard、失败汇总、有序 batch 和 retryable ordered apply。
- `tests/runtime_stress_tests.cpp`：高并发 init/shutdown/start_task stress，CI 中也用于 TSAN job。
- `benchmarks/io_*_benchmarks.cpp`、`benchmarks/queue_benchmarks.cpp` 与 `benchmarks/runtime_benchmarks.cpp`：IO adapter、文件系统、zero-copy、file/fixed-file、vectored、底层结构和 runtime 路径分开压测；公共 fake runtime 放在 `benchmarks/io_benchmark_support.hpp`。
- `benchmarks/perf_baseline.json`：本地 runtime benchmark baseline。
- `benchmarks/perf_baseline_github_ubuntu.json` 与 `scripts/check_benchmark_regression.py`：GitHub Ubuntu runner 性能 baseline 与回归阈值检查。

## 业务 IO adapter 接入建议

- 日志：`AsyncLogConfig::ordering` 默认 `LogOrdering::Ordered`，所有生产者进入单个 bounded MPSC，由 runtime 绑定的消费者按入队线性化顺序批量写后端；追求极致吞吐时可显式切到 `LogOrdering::Relaxed`，runtime 线程走 SPSC lane，框架外线程走 sharded MPSC。
- Redis/MySQL/Kafka/配置中心/服务发现：优先选择可暴露底层 fd 的 nonblocking client；将连接归属到固定 IO 线程，在 task 内维护状态机，按 WANT_READ/WANT_WRITE 注册 readiness；业务回调尽量只做轻量解析，重 CPU 处理再切回逻辑线程。
- RPC：连接归属固定 IO 线程，网络读写和 buffer 生命周期都留在该线程；拆包/编解码可先在 IO 线程做轻量 framing，再把 payload 切回逻辑线程处理，响应结果再切回 IO 线程发送。
