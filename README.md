# AsyncFlow

AsyncFlow 是一个基于显式线程布局的 C++20 异步任务框架。它不是通用线程池，而是固定 executor/event-loop 模型：业务先声明线程组，再把任务、网络服务、日志消费者等组件绑定到明确的框架线程上运行。

## 线程布局

业务使用 tag + `thread_layout` 声明线程组，不再手写连续递增 enum：

```cpp
struct LogicTag;
struct IoTag;
struct LogTag;

struct AppRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LogicTag, 4, af::ThreadKind::Worker, "logic">(),
        af::thread_group<IoTag, 2, af::ThreadKind::Io, "io">(),
        af::thread_group<LogTag, 1, af::ThreadKind::Log, "log">());

    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
    static constexpr std::size_t io_wait_reserve = 1024;
};

using Runtime = af::AsyncRuntime<AppRuntimeTraits>;
using Thread = Runtime::Thread;

inline constexpr auto logic_threads = Runtime::thread_group<LogicTag>();
inline constexpr auto io_threads = Runtime::thread_group<IoTag>();

struct AppThreads {
    static constexpr Thread Logic_0 = logic_threads.template at<0>();
    static constexpr Thread IO_0 = io_threads.template at<0>();
};

inline Thread player_thread(std::uint64_t player_id) noexcept {
    return logic_threads.shard(player_id);
}
```

`Thread` 只保存一个 `uint16_t` index。普通调度热路径直接用 index 定位 executor 和队列；`thread_group` 的 `begin/count/at()/shard()` 都是轻量整数运算。runtime 启动 executor 时会给系统线程命名，例如 `af-logic-0`、`af-io-1`，方便调试和线上排查。

`ThreadKind::Io` 是跨平台 IO 线程能力：Linux 使用 epoll + eventfd，macOS/BSD 使用 kqueue。Linux 也可以显式写 `ThreadKind::Epoll`，macOS/BSD 可以显式写 `ThreadKind::Kqueue`。

## 任务模型

业务任务继承 `Runtime::Task`，构造函数第一个参数必须是 `Task::FactoryToken`。任务由 runtime 创建、调度和释放，业务不要直接 `new/delete` 或栈上构造任务。

```cpp
class LoginTask final : public Runtime::Task {
public:
    explicit LoginTask(FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id) {
        player_id_ = player_id;
        return schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            state_ = State::Finish;
            return pending_on(AppThreads::IO_0);
        case State::Finish:
            return done();
        }
        return done();
    }

    enum class State { Start, Finish };
    State state_{State::Start};
    std::uint64_t player_id_{0};
};
```

推荐两步启动：

```cpp
auto task = Runtime::make_task<LoginTask>();
const bool started = task->do_it(player_id);
```

`schedule_fast()` / `pending_fast()` 表示 runtime 线程内快速投递；`schedule_ordered()` / `pending_ordered()` 强制走目标 MPSC 顺序路径。自身投递到自身时也可以显式选择快速路径或顺序路径。

## IO 与网络

普通 fd helper 走 native readiness：

- `af::io_read_some()` / `af::io_write_some()`
- `af::io_recv_some()` / `af::io_send_some()`
- `af::io_accept_some()` / `af::io_connect()` / `af::io_shutdown()`
- `af::io_recv_from_some()` / `af::io_send_to_some()`
- `af::io_readv_some()` / `af::io_writev_some()`
- `af::io_recvv_some()` / `af::io_sendv_some()`
- `af::io_sendfile_some()` / `af::io_splice_some()`

这些 helper 必须在 fd 所属 IO 线程上调用。热路径先尝试一次非阻塞 syscall；遇到 `EAGAIN` / `EWOULDBLOCK` 后注册一次性 readiness，并让原 task 在同一个 IO executor 上恢复。

新网络层使用 reactor-driven 设计。IO 线程直接管理 listener、connection、fd 事件、读写 buffer 和连接状态；业务 handler 在 IO 线程拿到字节流，再按需要创建任务切到逻辑线程处理。TCP/UDP/Unix socket 服务器、客户端都应绑定到一个或多个框架 IO 线程。

TCP/UDP/Unix socket 的控制面是 reactor-only：`bind_threads()`、`add_listener()`、`connect()`、`start()`、`remove_listener()`、`stop()` 应在同一个 IO reactor 任务内调用。外部线程应显式投递控制任务；这是无锁控制面的使用契约，框架不为外部直接调用额外建立同步兼容层。

TCP server 示例见：

- `examples/net_tcp_echo_server.cpp`
- `examples/net_tcp_login_server.cpp`

## 日志

异步日志消费者可以绑定到 runtime 线程，不额外创建独立消费线程。runtime 线程内生产者走 SPSC lane，外部线程走 MPSC ingress；后端可以写文件，也可以通过 UDP/TCP 后端发送。日志格式使用 Abseil 前端格式化，runtime task id 会插入用户日志字段开头。

## 性能边界

- 固定 runtime 线程之间使用 bounded SPSC ring。
- 外部入口使用 bounded MPSC queue。
- executor 自身投递自身默认可走本地 queue，也可显式走顺序 MPSC。
- 队列满策略由 traits 分别配置 runtime 生产者和外部生产者。
- task 对象来自按类型分离的对象池，跨线程释放走批量回收。
- IO 线程同时处理 native readiness 和调度到本线程的 task，阻塞前会先 drain 可运行任务。
- epoll/kqueue 注册只在 fd 注册、interest 变化、取消和关闭时发生，普通事件处理尽量避免重复系统调用。

## 构建与测试

```sh
conan install . --output-folder=build-conan --build=missing -s build_type=Release
cmake -S . -B build-conan/build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build-conan/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-conan/build/Release --parallel
ctest --test-dir build-conan/build/Release --output-on-failure
```

常用目标：

```sh
cmake --build build-net-debug/build/Debug --target \
  asyncflow_runtime_tests \
  asyncflow_log_tests \
  asyncflow_runtime_stress_tests \
  asyncflow_net_tcp_echo_server_example \
  asyncflow_net_tcp_login_server_example
```
