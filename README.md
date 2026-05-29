# AsyncFlow

一个基于固定线程枚举的轻量 C++20 异步任务框架。

## 线程定义

业务使用连续递增 enum 定义线程，最后一个枚举值必须是 `enum_num_end`：

```cpp
enum class AppThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    IO_0,
    enum_num_end,
};

struct AppRuntimeTraits {
    using Thread = AppThread;
    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AppThread::enum_num_end);

    static constexpr AppThread logic_begin = AppThread::Logic_0;
    static constexpr std::uint16_t logic_count = 4;

    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using Runtime = af::AsyncRuntime<AppRuntimeTraits>;
using Task = Runtime::Task;
```

## 任务 API

业务任务继承 `Runtime::Task`，通过 `schedule()` 首次调度，通过 `pending_on()` 切线程挂起，通过 `again()` 在当前线程继续，通过 `failed()` 失败结束并释放任务。`schedule()`、`Runtime::make_task()` 之后调用的启动函数、以及 `Runtime::start_task()` 都能表达入队是否成功，队列满时不会无限增长。

```cpp
class LoginTask final : public Task {
public:
    explicit LoginTask(Task::FactoryToken token) : Task(token) {}

    void do_it(std::uint64_t player_id) {
        player_id_ = player_id;
        schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            state_ = State::QueryDb;
            return pending_on(AppThread::DB_0);
        case State::QueryDb:
            state_ = State::BackToLogic;
            return pending_on(player_thread(player_id_));
        case State::BackToLogic:
            state_ = State::Finish;
            return again();
        case State::Finish:
            return done();
        }
        return done();
    }

    enum class State { Start, QueryDb, BackToLogic, Finish };
    State state_{State::Start};
    std::uint64_t player_id_{0};
};
```

每个任务构造函数必须把 `Task::FactoryToken` 作为第一个参数并传给基类。这个 token 只有 runtime 能生成，因此业务代码无法直接在栈上构造任务，也无法直接 `new` 任务。业务侧也不要直接 `delete` 任务对象。

推荐的显式启动方式是通过框架工厂创建受管任务对象，再调用任务自己的启动函数；这个函数可以叫 `do_it()`，也可以是业务自定义名称，只要在函数里完成首次 `schedule()`：

```cpp
auto task = Runtime::make_task<LoginTask>();
if (!task->do_it(player_id)) {
    // 未调度成功时，task handle 析构会自动回收到对象池。
}
```

`make_task<T>()` 返回的是一个轻量包装句柄，内部由 runtime 统一接入对象池、生命周期引用和后续可替换的内存池策略。保留的便捷写法等价于“创建后调用 `do_it()`”：

```cpp
Runtime::start_task<LoginTask>(player_id);
```

## 性能边界

- runtime 固定线程之间使用 bounded SPSC ring，每个 source -> target 一条队列。
- 非 runtime 线程进入 executor 时使用 bounded MPMC ingress，用于 `start_task()` 等外部入口。
- 队列容量由 traits 配置；`QueueFullPolicy::Reject` 直接返回失败，`QueueFullPolicy::Yield` 会让出 CPU 等待空位。
- `make_task<T>()` / `start_task<T>()` 使用按任务类型分离的无锁 free-list 对象池，任务完成后回收到对应类型池；`create_task<T>()` 作为兼容别名保留。
- executor 空闲等待使用 C++20 `std::atomic::wait/notify_one`。
- Task 生命周期由状态机保护，debug 下会检查重复调度、完成后调度、运行中重复唤醒。
- `parallel_shards_ordered()` 会对每个 shard 维护 `last_applied_batch_id`，要求 batch id 连续递增。
- `start_ordered_task<Stream, ApplyTask>()` 会在指定 sequencer 线程上缓存乱序 batch，并按 batch_id 连续启动 apply task。
- `parallel_shards()` 的 handler 如果返回 `bool`，`false` 会计为 shard 失败；owner 恢复后可用 `last_parallel_failures()` 读取失败数。
- `TaskResult::Cancelled` 可用于取消结束；runtime 未初始化或 stopping 时 `start_task()` 返回失败并销毁任务。
- 热路径不使用 `std::function`，shard handler 通过模板静态绑定。

## 构建与测试

```sh
conan install . --output-folder=build-conan --build=missing -s build_type=Release
cmake --preset conan-release
cmake --build --preset conan-release --parallel
ctest --test-dir build-conan/build/Release --output-on-failure
./build-conan/build/Release/asyncflow_runtime_benchmarks --benchmark_min_time=0.005s
```
