# AsyncFlow

AsyncFlow 是一个 C++17 异步任务与网络框架。它不是通用线程池，而是固定线程布局的 runtime：业务先声明 IO/CPU 线程组，再把 task、reactor、网络服务和异步日志消费者放到明确的框架线程上执行。

## 线程布局

线程布局使用结构化配置声明。线程只有 `io` 和 `cpu` 两类，runtime 会按配置创建固定数量的系统线程，并设置可读的线程名，方便 `top`、调试器和日志排查。

```cpp
af::runtime_config config;
config.threads = {
    af::io_threads("net-io", 2),
    af::cpu_threads("logic", 4),
};
config.scheduler.task_drain_budget = 256;
config.reactor.backend = af::reactor_backend::auto_select;
config.reactor.edge_triggered = false;

af::runtime runtime(config);
runtime.start();

af::thread_group_ref io = runtime.io_threads();
af::thread_group_ref logic = runtime.thread_group("logic");
af::thread_ref player_thread = logic.shard(player_id);
```

`thread_ref` 只保存一个 `uint16_t` index，`thread_group_ref::front()`、`at()`、`shard()` 都是轻量整数操作。Linux 默认选择 epoll，macOS/BSD 默认选择 kqueue，必要时也可以显式选择 select 后端。

## 任务模型

业务 task 继承 `af::runtime_task`。task 只能通过 `af::make_task<T>(runtime, ...)` 创建，runtime 在内部接管对象池、task id、生命周期和跨线程释放。

推荐启动方式是两段式：先创建对象，再手动调用 task 的 `do_it()` 填充业务参数并调度。

```cpp
class login_task final : public af::runtime_task {
public:
    login_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(std::uint64_t player_id, af::thread_group_ref logic_threads) noexcept {
        player_id_ = player_id;
        return schedule_to(logic_threads.shard(player_id_));
    }

private:
    af::task_result run_task() noexcept override {
        // 业务逻辑可以继续 pending_to()/pending_after() 切换线程或等待定时器。
        return done();
    }

    std::uint64_t player_id_{0};
};

auto task = af::make_task<login_task>(runtime);
const bool started = task->do_it(player_id, logic_threads);
```

调度 API 使用 `schedule` 语义：

- `schedule_to(thread)`：把新创建或 pending 状态的 task 投递到目标线程。
- `schedule_after(thread, delay)` / `schedule_at(thread, time)`：把 task 挂到目标线程本地 timer，定时到期后进入该线程 inbox。
- `pending_to(thread)`：在 `run_task()` 中声明当前 task 暂停，下一步切到目标线程继续执行。
- `pending_after(delay)` / `pending_at(time)`：在 `run_task()` 中让出当前线程，定时恢复。

所有 task 投递都进入目标 executor 的 intrusive MPSC inbox，不再保留隐藏 local queue 或 SPSC 双路径，因此自身投递自身也保持统一顺序语义。

## IO 与网络

旧的公开 `af/io*.hpp` task 级 helper 已移除。当前推荐使用 runtime-native 网络层：

- `af::net::tcp_server`
- `af::net::tcp_client`
- `af::net::udp_socket`
- `af::net::unix_*`

每个 IO 线程拥有一个 reactor。reactor 对上屏蔽 epoll/kqueue/select 差异，对下管理 listener、connection、channel、timer wakeup 和 fd interest。网络对象的控制面约定在所属 reactor 线程执行，外部线程需要先通过 `runtime.post(io_thread, ...)` 切到对应线程。

TCP server 的典型流程：

```cpp
af::net::tcp_server_config server_config;
server_config.connection.read_budget_bytes = 512U * 1024U;
server_config.connection.output_high_watermark = 8U * 1024U * 1024U;

af::net::tcp_server server(runtime, server_config);

runtime.post(io.front(), [&] {
    af::net::tcp_connection_callbacks callbacks;
    callbacks.owner = &state;
    callbacks.on_read = &on_read;
    callbacks.on_close = &on_close;

    af::net::tcp_listener_config listener;
    listener.endpoint = af::net::tcp_endpoint::any(8080);
    listener.threads = {io.front()};
    listener.options.reuse_port = true;

    server.add_listener(std::move(listener), callbacks);
    server.start();
});
```

读事件在 IO 线程回调。业务可以先解析字节流，再创建 task 切到 CPU 线程处理，处理完成后通过 `tcp_connection_handle::send()` 把写入调度回连接所属 reactor。

## 日志

异步日志通过 `runtime_config.logger` 配置，不需要额外启动独立消费线程。日志 consumer 是绑定到 runtime 线程的 service task，随 `runtime.start()` 启动，随 `runtime.stop()` drain/flush。

```cpp
using namespace std::chrono_literals;

af::runtime_config config;
config.threads = {
    af::cpu_threads("logic", 2),
    af::io_threads("log-io", 1),
};
config.logger = af::log_config::ordered();
config.logger.consumer_thread = af::thread_selector::io(0);
config.logger.queue_capacity = 1U << 15U;
config.logger.max_batch_records = 512;
config.logger.max_batch_delay = 1000us;
config.logger.backends = {
    af::file_log_backend_config{"server.log", true, false, 64},
};
```

ordered 日志默认提供全局 sequence，consumer 按批 merge 后尽量保持后端写入顺序；relaxed 日志适合只追求吞吐、不要求跨线程全局顺序的场景。用户日志入口使用 `AF_LOG` / `AF_LOG_IF`，日志等级是运行时可设置的，等级不匹配的日志不会进入格式化热路径。

## 性能边界

- task 投递使用 intrusive unbounded MPSC inbox，避免队列满导致非首次调度 task 卡死。
- 异步日志前端使用 bounded ring MPSC，队列满时按配置执行丢弃或阻塞策略。
- task 对象池和 log record 对象池使用线程本地 cache + slab，跨线程释放批量回收。
- executor 空闲等待默认使用 futex/atomic wait，CPU 密集线程可配置 spin/yield 策略。
- reactor 默认 LT 模式，只在 fd 注册、interest 变化、取消和关闭时调用 poller 控制系统调用。
- 热路径结构按 cache line 对齐，队列游标、统计计数和跨线程共享字段分离，减少 false sharing。

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
