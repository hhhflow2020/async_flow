# AsyncFlow C++ 异步框架需求与设计

本文档描述 AsyncFlow 的需求、设计实现、公开 API、性能边界、测试与示例。当前实现是一个基于 C++20 的固定线程异步任务框架，框架名为 `AsyncFlow`，命名空间为 `af`。

## 1. 目标

AsyncFlow 不是通用线程池，而是固定 Executor/EventLoop 模型：

```text
AsyncRuntime + Fixed Threads + Task + Scheduler + Shard + Ordered Batch
```

核心目标：

- 业务线程在编译期通过连续递增 enum 定义。
- 任务对象由框架创建、调度、释放，业务不直接 `new` / `delete` / 栈上创建任务。
- 任务内部可写状态机，可跨固定线程挂起恢复。
- 同一个业务 key 始终路由到同一个逻辑线程，减少共享数据加锁。
- 支持 bounded 队列，不能无限积压任务。
- 固定线程间使用 SPSC 队列，外部入口使用 bounded MPSC 队列。
- 支持无序 shard 批处理，只调度非空 shard。
- 支持有序 batch，全 shard 都推进 `last_applied_batch_id`。
- 使用模板静态绑定 handler 和任务类型，减少虚函数之外的间接调用、分支和动态分配。
- 使用现代 CMake + Conan 管理构建、测试和 benchmark。

## 2. 线程定义

业务使用连续递增 enum 定义固定线程。建议使用 signed underlying type，并保留首尾哨兵：

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
```

规则：

- `enum_thread_index_start = -1` 只作为边界哨兵，不参与调度。
- 第一个真实线程默认索引为 0。
- `enum_thread_index_end` 的值等于真实线程数量，可直接作为 `thread_count`。
- 业务分组如 logic shard 数量不放入 runtime traits，而是在业务配置处根据 enum 计算。

示例：

```cpp
struct AppRuntimeTraits {
    using Thread = AppThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AppThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;

inline constexpr AppThread player_logic_begin = AppThread::Logic_0;
inline constexpr std::uint16_t player_logic_shard_count =
    static_cast<std::uint16_t>(
        async::thread_index(AppThread::Logic_3) -
        async::thread_index(player_logic_begin) + 1U);

inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return async::shard_by<player_logic_begin, player_logic_shard_count>(player_id);
}
```

## 3. Runtime Traits

`af::AsyncRuntime<Traits>` 只消费框架层参数。

必填项：

```cpp
using Thread = AppThread;
static constexpr std::uint16_t thread_count = ...;
```

可选项：

```cpp
static constexpr std::size_t spsc_queue_capacity = 1024;
static constexpr std::size_t external_queue_capacity = 1024;
static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
```

默认值：

- `spsc_queue_capacity = 1024`
- `external_queue_capacity = spsc_queue_capacity`
- `queue_full_policy = QueueFullPolicy::Reject`
- `shutdown_policy = ShutdownPolicy::WaitForTasks`

## 4. Task 模型

业务任务继承 `async::Task`。任务构造函数必须接收 `Task::FactoryToken`，这个 token 只有 runtime 能创建，因此业务不能直接构造任务。

```cpp
class AddGoldTask final : public Task {
public:
    explicit AddGoldTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id, int gold) {
        player_id_ = player_id;
        gold_ = gold;
        return schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        return done();
    }

    std::uint64_t player_id_{0};
    int gold_{0};
};
```

任务创建推荐两步式：

```cpp
auto task = async::make_task<AddGoldTask>();
const bool started = task->do_it(player_id, 100);
```

也可使用便捷入口：

```cpp
const bool started = async::start_task<AddGoldTask>(player_id, 100);
```

### 4.1 生命周期约束

`BasicTask` 删除了 public `new` / `new[]`，构造函数受 `FactoryToken` 保护。

业务侧不允许：

```cpp
auto* task = new AddGoldTask(...);
AddGoldTask task(...);
delete task;
```

框架内部通过按任务类型分离的对象池创建和销毁任务。`make_task<T>()` 返回 `TaskHandle<T>`，用于管理首次调度前和已完成但 handle 未释放时的生命周期。

### 4.2 TaskResult

```cpp
enum class TaskResult : std::uint8_t {
    Done,
    Pending,
    Again,
    Failed,
    Cancelled,
};
```

语义：

- `Done`：任务正常完成，runtime 释放执行生命周期引用。
- `Failed`：任务失败结束，释放方式与 `Done` 一致，并计入 shard handler 失败时的 failure 统计。
- `Cancelled`：任务取消结束，释放方式与 `Done` 一致。
- `Pending`：任务进入挂起状态，等待后续 `post()`、`pending_on()`、parallel group 或外部事件恢复。
- `Again`：任务在当前 runtime 线程重新排队，继续推进状态机。

### 4.3 Task 基类 API

任务内部可使用的 protected API：

```cpp
bool schedule(Thread thread) noexcept;
TaskResult pending_on(Thread thread) noexcept;

static TaskResult done() noexcept;
static TaskResult pending() noexcept;
static TaskResult again() noexcept;
static TaskResult failed() noexcept;
static TaskResult cancelled() noexcept;

static bool runtime_stopping() noexcept;
static Thread current_thread() noexcept;
static bool is_current(Thread thread) noexcept;

std::uint32_t last_parallel_failures() const noexcept;
```

建议：

- 首次启动函数可命名为 `do_it()`，也可以是业务自定义名称。
- 启动函数里必须完成首次 `schedule()`。
- 跨线程继续执行优先使用 `pending_on(thread)`，隐藏 `Runtime::post()` 细节。
- `run()` 不允许抛异常；debug 下会断言。

## 5. Runtime API

初始化和关闭：

```cpp
async::init();
async::shutdown();
async::wait_for_idle();
```

任务创建：

```cpp
auto task = async::make_task<MyTask>(ctor_args...);
auto task = async::create_task<MyTask>(ctor_args...); // 兼容别名
bool ok = async::start_task<MyTask>(do_it_args...);
```

调度与线程信息：

```cpp
bool ok = async::post(AppThread::Logic_0, task);
AppThread thread = async::current_thread();
std::uint16_t index = async::current_thread_index();
bool runtime_thread = async::is_runtime_thread();
bool stopping = async::is_stopping();
```

线程索引转换：

```cpp
std::uint16_t index = async::thread_index(AppThread::Logic_0);
AppThread thread = async::thread_from_index(index);
```

分片：

```cpp
AppThread thread = async::shard_by<player_logic_begin, player_logic_shard_count>(player_id);
auto sharded = async::split_by_shard(std::move(ops), shard_count, key_fn);
```

运行期观测：

```cpp
std::uint32_t count = async::unfinished_task_count();
std::uint64_t batch_id = async::ordered_last_applied_batch_id(AppThread::Logic_0);
```

## 6. 调度实现

### 6.1 固定线程

`AsyncRuntime` 在 `init()` 时创建 `thread_count` 个 executor。每个 executor 绑定一个固定线程索引，并在该线程内循环执行任务。

### 6.2 队列结构

调度路径：

- runtime 线程调度到自己：executor 本地 bounded queue。
- runtime 线程调度到其它 runtime 线程：`source -> target` 的 bounded SPSC ring。
- 非 runtime 线程进入 runtime：每个 target 一个 bounded MPSC ingress queue。

SPSC 队列使用 head/tail cache 减少共享 cache line 读取；队列容量会向上取 power-of-two，使用 mask 替代取模。

### 6.3 满队列策略

```cpp
enum class QueueFullPolicy : std::uint8_t {
    Reject,
    Yield,
};
```

- `Reject`：队列满时调度失败，`schedule()` / `start_task()` 返回 `false`，框架回滚任务状态并释放未启动任务。
- `Yield`：队列满时让出 CPU 并等待空位，适合业务必须接收任务但可以接受短暂等待的入口。

本地队列阻塞入队时，如果目标就是当前 executor，会优先执行本地已有任务，避免固定容量队列满后等待自己。

### 6.4 active post 与 shutdown

`shutdown()` 会先把状态切换到 `Stopping`，等待已经进入 `post()` 的外部调度退出，然后根据 shutdown policy 决定是否等待任务完成。

runtime 线程在 `WaitForTasks` 的 stopping 阶段仍允许恢复已经被接收的任务，例如任务在 `run()` 中调用 `pending_on()` 切到另一个线程继续完成。外部新任务在 stopping 后会被拒绝。

## 7. Shutdown 策略

```cpp
enum class ShutdownPolicy : std::uint8_t {
    WaitForTasks,
    StopImmediately,
};
```

### 7.1 WaitForTasks

默认策略。语义：

- 首次成功调度任务时计入 unfinished task。
- 任务进入 `Done` / `Failed` / `Cancelled` 时计数减少。
- `shutdown()` 等待 unfinished task 变为 0 后再停止 executor。
- 已被接收的任务可以在 stopping 期间通过 runtime 线程继续调度完成。
- 外部新任务在 stopping 后被拒绝并释放。

适合大多数服务正常退出路径。

### 7.2 StopImmediately

快速退出策略。语义：

- `shutdown()` 不等待 unfinished task。
- 不为任务完成路径维护 unfinished task 计数，热路径更轻。
- 默认不追踪 pending task，适合进程即将退出、业务不要求 pending 任务完整清理的路径。
- 如果 traits 设置 `enable_task_registry = true`，首次成功调度的任务会进入 intrusive registry；`shutdown()` 停止并 join executor 后，会取消并释放仍处于 `Pending` / `Queued` 的任务。

未开启 registry 时 task 不携带 registry 链接字段；开启 registry 会增加首次调度和任务完成路径上的轻量锁成本，适合需要“不等待但仍销毁 pending task”的服务退出路径。

## 8. 批处理与 Shard

### 8.1 ShardedOps

```cpp
template <typename Op>
struct ShardedOps {
    std::vector<std::vector<Op>> shards;
};
```

分片工具：

```cpp
auto sharded = async::split_by_shard(
    std::move(ops),
    player_logic_shard_count,
    [](const Op& op) {
        return op.player_id;
    });
```

### 8.2 ParallelMode

```cpp
enum class ParallelMode : std::uint8_t {
    NonEmptyOnly,
    AllShards,
};
```

- `NonEmptyOnly`：只调度非空 shard，适合普通无序批处理。
- `AllShards`：所有 shard 都调度，包括空 shard，适合有序 batch。

### 8.3 无序 parallel_shards

显式 shard 起点：

```cpp
async::parallel_shards(
    player_logic_begin,
    sharded_ops,
    af::ParallelMode::NonEmptyOnly,
    this,
    handler);
```

默认从线程 0 开始：

```cpp
async::parallel_shards(
    sharded_ops,
    af::ParallelMode::NonEmptyOnly,
    this,
    handler);
```

handler 签名：

```cpp
void handler(std::uint16_t shard, std::vector<Op>& ops);
bool handler(std::uint16_t shard, std::vector<Op>& ops);
```

如果 handler 返回 `bool`，`false` 计为该 shard 失败。owner 恢复后可通过 `last_parallel_failures()` 读取失败数量。

### 8.4 有序 parallel_shards_ordered

显式 shard 起点：

```cpp
async::parallel_shards_ordered(
    player_logic_begin,
    sharded_ops,
    batch_id,
    this,
    handler);
```

默认从线程 0 开始：

```cpp
async::parallel_shards_ordered(
    sharded_ops,
    batch_id,
    this,
    handler);
```

兼容需求文档里的 `parallel_shards(..., AllShards, batch_id, owner, handler)`：

```cpp
async::parallel_shards(
    sharded_ops,
    af::ParallelMode::AllShards,
    batch_id,
    this,
    handler);
```

handler 签名：

```cpp
void handler(std::uint16_t shard, std::vector<Op>& ops, std::uint64_t batch_id);
bool handler(std::uint16_t shard, std::vector<Op>& ops, std::uint64_t batch_id);
```

有序规则：

- 每个 shard 维护 `last_applied_batch_id`。
- 执行时检查 `batch_id == last_applied_batch_id + 1`。
- handler 成功时推进当前 shard 的 `last_applied_batch_id`。
- handler 失败时不推进失败 shard，owner 通过 `last_parallel_failures()` 得知失败数。
- 空 shard 也会执行 no-op，并推进 batch id。

### 8.5 start_ordered_task

`start_ordered_task<StreamTag, ApplyTaskT>()` 在指定 sequencer 线程上按 `batch.batch_id` 连续启动 apply task。

```cpp
bool ok = async::start_ordered_task<PlayerDeltaStream, ApplyPlayerDeltaBatchTask>(
    AppThread::Logic_0,
    std::move(batch));
```

语义：

- batch id 小于当前期待值：视为旧 batch，忽略。
- batch id 等于当前期待值：立即启动 apply task，然后尝试 drain 后续连续 batch。
- batch id 大于当前期待值：缓存，等待缺失 batch 到达。
- apply task 启动失败：不推进期待 batch id，业务可重试同一个 batch。
- runtime 重启后，ordered start 状态按 generation 重置。

## 9. CRUD Batch Helper

框架提供轻量 CRUD 数据结构，不引入额外运行期状态。

```cpp
enum class OpType : std::uint8_t {
    Add,
    Update,
    Delete,
};

template <typename Key, typename Value>
struct CrudOp {
    OpType type{OpType::Add};
    Key key{};
    Value value{};
};

template <typename Key, typename Value>
struct ChangeBatch {
    std::uint64_t batch_id{0};
    std::vector<CrudOp<Key, Value>> ops;
};
```

拆分 CRUD op：

```cpp
auto sharded = af::split_crud_ops(std::move(ops), shard_count);

auto sharded = af::split_crud_ops(
    std::move(ops),
    shard_count,
    [](const Key& key) {
        return custom_hash(key);
    });
```

拆分 ChangeBatch：

```cpp
auto sharded = af::split_change_batch(batch, shard_count);
```

`split_change_batch()` 会保留 `batch.batch_id`，并消费 `batch.ops`。

## 10. 性能设计

关键实现选择：

- 固定线程模型让业务数据按 owner thread 归属，减少业务锁。
- runtime 线程之间使用 bounded SPSC ring，避免 MPMC 在固定生产者/消费者场景下的额外 CAS 争用。
- 外部生产者入口仍使用 bounded MPSC queue，隔离非 runtime 线程。
- 队列全部 bounded，避免无限增长。
- SPSC head/tail 与 cache 使用 cache-line 对齐，减少 false sharing。
- runtime 全局状态、active posts、unfinished task、generation 也按 cache line 对齐。
- handler 使用模板静态绑定，不使用 `std::function`。
- task 使用按类型分离对象池，减少频繁 malloc/free。
- executor 空闲等待使用 C++20 `std::atomic::wait/notify_one`。
- `StopImmediately` 不维护 task unfinished 计数，避免不等待策略承担额外热路径原子操作。
- `StopImmediately` 可通过 `Traits::enable_task_registry = true` 开启 intrusive task registry，shutdown 后会取消并释放仍处于 `Pending` / `Queued` 的任务。
- 有序 batch 提供 `af::retryable_ordered_batch_options`，业务重试同一 batch 时可跳过已经应用成功的 shard。
- Linux IO executor 使用 epoll + eventfd，任务先调度到指定 IO 线程后再注册 fd readiness，避免所有 IO 都通过 MPMC 队列跨线程搬运。
- `ThreadKind::IoUring` 优先初始化 io_uring，并通过 eventfd 唤醒 completion；如果 io_uring 不可用，线程仍保留 epoll readiness fallback。
- `af::IoFile::read_at()` / `write_at()` / `readv_at()` / `writev_at()` / `fsync()` 通过 io_uring 提交真正的文件异步操作，completion 后恢复原 task。
- `af::TcpListener::accept_some()` / `af::TcpStream::connect()` / `recv_some()` / `send_some()` / `recvv_some()` / `sendv_some()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT` / `IORING_OP_RECV` / `IORING_OP_SEND` 或 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`；`af::UdpSocket::recv_from_some()` / `send_to_some()` / `recvv_from_some()` / `sendv_to_some()` 优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，ring 不可用或 would-block 时退回 epoll readiness。
- `af::IoTimer` 使用 Linux `timerfd` readiness，适合超时、重试、心跳和连接保活，`ThreadKind::IoUring` 线程也可复用 epoll fallback。
- `af::IoFile` / `af::TcpListener` / `af::TcpStream` / `af::UdpSocket` / `af::IoTimer` 仅保存 `thread + fd`，内联转发到 IO helper，不拥有 fd、不分配堆内存、不增加额外分支表。

仍需注意：

- `WaitForTasks` 为了 shutdown 可等待，会在任务首次成功调度和最终完成时维护一个全局 unfinished counter。
- 有序 batch 失败后的重试、跳过和补偿策略仍由业务决定；`af::OrderedBatchRetrySkipPolicy` 只负责记录失败次数并给出 Retry / Skip / Stop 决策。
- 未开启 `enable_task_registry` 时，`StopImmediately` 仍保持最小热路径开销，不追踪 pending task。

## 11. 示例

示例统一放在 `examples/`。

### 11.1 app_runtime.hpp：共享运行时配置

文件：`examples/app_runtime.hpp`

展示内容：

- 使用 enum 定义固定线程。
- 使用 `AppRuntimeTraits` 配置 runtime。
- 定义 `using async = af::AsyncRuntime<AppRuntimeTraits>`。
- 计算 `player_logic_shard_count`。
- 使用 `async::shard_by<Begin, Count>(key)` 实现 player 到 logic shard 的路由。

关键代码：

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

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
```

### 11.2 basic.cpp：基础任务与状态机

文件：`examples/basic.cpp`

展示内容：

- 两步式创建任务：`make_task<T>() + do_it()`。
- `schedule(thread)` 首次调度。
- `pending_on(thread)` 在状态机里跨线程切换。
- `again()` 在当前线程继续执行下一状态。
- `shutdown()` 在 `WaitForTasks` 策略下等待已接收任务完成。

涉及任务：

- `AddGoldTask`：按 player id 调度到所属 logic 线程，执行一次后完成。
- `LoginTask`：从 logic 线程切到 DB 线程，再回到 player logic 线程，最后用 `again()` 完成状态机。

运行方式：

```sh
./build-conan/build/Release/asyncflow_basic_example
```

### 11.3 parallel_shards.cpp：普通无序 shard 批处理

文件：`examples/parallel_shards.cpp`

展示内容：

- 将一批 `AddGoldOp` 按 `player_id` 拆分到 player logic shard。
- 使用 `ParallelMode::NonEmptyOnly` 只调度有数据的 shard。
- shard handler 在目标 runtime 线程上执行。
- owner task 返回 `pending()`，等待所有 shard 完成后恢复。

核心 API：

```cpp
sharded_ops_ = async::split_by_shard(
    std::move(ops),
    player_logic_shard_count,
    [](const AddGoldOp& op) { return op.player_id; });

async::parallel_shards(
    player_logic_begin,
    sharded_ops_,
    af::ParallelMode::NonEmptyOnly,
    this,
    handler);
```

运行方式：

```sh
./build-conan/build/Release/asyncflow_parallel_shards_example
```

### 11.4 ordered_batches.cpp：有序 batch 与全 shard 屏障

文件：`examples/ordered_batches.cpp`

展示内容：

- 外部提交 batch 可以乱序到达。
- `start_ordered_task<StreamTag, ApplyTask>()` 在 sequencer 线程上缓存乱序 batch。
- apply task 使用 `parallel_shards_ordered()`，确保每个 logic shard 都按 batch id 连续推进。
- 即使某个 shard 当前 batch 没有数据，也会执行 no-op 并更新版本。

涉及任务：

- `SubmitPlayerDeltaBatchTask`：在 IO 线程提交 batch 到 ordered stream。
- `ApplyPlayerDeltaBatchTask`：按 batch id 有序应用到所有 player logic shard。

核心 API：

```cpp
async::start_ordered_task<PlayerDeltaStream, ApplyPlayerDeltaBatchTask>(
    AppThread::Logic_0,
    std::move(batch));

async::parallel_shards_ordered(
    player_logic_begin,
    sharded_deltas_,
    batch_.batch_id,
    this,
    handler);
```

运行方式：

```sh
./build-conan/build/Release/asyncflow_ordered_batches_example
```

### 11.5 crud_apply.cpp：完整 CRUD apply 业务模板

文件：`examples/crud_apply.cpp`

展示内容：

- 使用 `af::ChangeBatch<Key, Value>` 表示 Add / Update / Delete 变更流。
- 外部乱序提交 batch，IO sequencer 使用 `start_ordered_task` 缓存并按 batch id 启动 apply task。
- apply task 使用 `split_change_batch()` 拆分到 player logic shard。
- `parallel_shards_ordered(..., af::retryable_ordered_batch_options, ...)` 支持失败后重试同一 batch 时跳过已成功 shard。

运行方式：

```sh
./build-conan/build/Release/asyncflow_crud_apply_example
```

### 11.6 io_adapters.cpp：TCP/UDP IO adapter 业务模板

文件：`examples/io_adapters.cpp`

展示内容：

- `af::TcpStream<AppThread>` 封装 stream socket 的 `recv/send` 状态机。
- `af::UdpSocket<AppThread>` 封装 datagram socket 的 `recvmsg/sendmsg`、`recvv_from/sendv_to` 或 `recvfrom/sendto` fallback 状态机。
- fd readiness、syscall、io_uring submit/completion 和任务恢复都发生在绑定的 IO executor 上。

运行方式：

```sh
./build-conan/build/Release/asyncflow_io_adapters_example
```

### 11.7 io_uring_file.cpp：文件异步 IO 业务模板

文件：`examples/io_uring_file.cpp`

展示内容：

- 使用 `ThreadKind::IoUring` 声明文件 IO 线程。
- 使用 `af::IoFile<FileThread>::write_at()` / `fsync()` / `read_at()` 完成文件写入、落盘和读取。
- io_uring 不可用时通过 `io_uring_backend_available()` 做显式降级。

运行方式：

```sh
./build-conan/build/Release/asyncflow_io_uring_file_example
```

### 11.8 io_uring_datagram.cpp：UDP io_uring 业务模板

文件：`examples/io_uring_datagram.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::UdpSocket` 完成 client/server round trip。
- `recv_from_some()` / `send_to_some()` 优先走 `recvmsg/sendmsg` completion，不可用时自动退回 epoll readiness。
- 每个状态拆成成员函数，避免把业务流程揉在一个 `switch` 分支里。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_datagram_example
```

### 11.9 io_tcp_connect_accept.cpp：TCP accept/connect 业务模板

文件：`examples/io_tcp_connect_accept.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::TcpListener` 和 `af::TcpStream` 完成 TCP server/client round trip。
- `accept_some()` / `connect()` 优先走 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT`，不可用时自动退回 epoll readiness。
- server/client 每个状态拆成成员函数，覆盖 accept、connect、send 和 recv 的常见业务骨架。

运行：

```sh
./build-conan/build/Release/asyncflow_io_tcp_connect_accept_example
```

### 11.10 io_vectored.cpp：scatter/gather stream/datagram 业务模板

文件：`examples/io_vectored.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::TcpStream::sendv_some()` / `recvv_some()` 完成两段 buffer 的 stream round trip，并用 `af::UdpSocket::sendv_to_some()` / `recvv_from_some()` 完成 datagram round trip。
- 协议头、正文或日志片段可以直接作为 `iovec` 数组发送，避免先拼到连续临时 buffer。
- `iovec` 数组和其指向的 buffer 在 pending IO 完成前必须保持有效，因此示例将它们保存为 task 成员。

运行：

```sh
./build-conan/build/Release/asyncflow_io_vectored_example
```

### 11.11 io_timer.cpp：timerfd 异步定时器业务模板

文件：`examples/io_timer.cpp`

该示例展示：

- `af::make_timerfd()` 创建非阻塞 timer fd，fd 生命周期仍由 `af::UniqueFd` 管理。
- `af::IoTimer::wait()` 在绑定 IO 线程上等待 timerfd readable，并在到期后恢复原 task。
- `af::arm_timerfd_after()` 可用于超时、重试、心跳和连接保活。

运行：

```sh
./build-conan/build/Release/asyncflow_io_timer_example
```

## 12. 测试覆盖

测试使用 GTest，入口目标是 `asyncflow_runtime_tests`。

文件布局：

- `tests/runtime_lifecycle_tests.cpp`：任务生命周期、状态机、背压、shutdown 策略。
- `tests/runtime_parallel_tests.cpp`：parallel shards、失败汇总、有序 batch、ordered start 边界、retryable ordered apply。
- `tests/runtime_stress_tests.cpp`：高并发 init/shutdown/start_task stress，可配合 TSAN 拉长运行。
- `tests/utility_tests.cpp`：SPSC/MPSC/MPMC 队列、对象池、分片工具、CRUD helper、BatchSequencer、ordered retry/skip policy。
- `tests/runtime_io_tests.cpp`：IO 线程、epoll readiness、timerfd、io_uring 文件、stream 和 datagram vectored 操作、io_uring TCP accept/connect/stream 与 UDP datagram 操作、read/write/TCP/UDP helper 与 adapter、重复 fd wait、HUP/EOF、非法 fd、worker 误用和 pending IO shutdown。

重点覆盖：

- 任务只能通过 runtime factory 创建。
- 未调度任务由 handle 回收。
- 完成任务在 handle 释放后销毁。
- `Done` / `Failed` / `Cancelled` 都能释放任务。
- `pending_on()` 跨线程恢复和 `again()` 当前线程继续。
- Reject 策略下队列满返回失败并销毁被拒绝任务。
- Yield 策略可处理多个外部生产者和同线程 fanout。
- Runtime 未初始化或 stopping 后拒绝外部新任务。
- `WaitForTasks` shutdown 等待已接收任务完成。
- `WaitForTasks` stopping 阶段允许 runtime 线程恢复已接收任务。
- `StopImmediately` 不等待 pending 任务；开启 registry 时会取消并销毁 pending task。
- `NonEmptyOnly` 跳过空 shard。
- `AllShards` 包含空 shard。
- shard handler 失败可被 owner 读取。
- ordered start 缓存乱序 batch、忽略重复/旧 batch、runtime 重启后重置。
- ordered batch 连续推进每个 shard 的 `last_applied_batch_id`。
- retryable ordered batch 跳过已经应用过同一 batch id 的 shard，只重跑仍落后的 shard。
- ordered batch handler 失败时失败 shard 不推进版本。
- epoll IO task 在 readable/writable、timerfd、UDP 零长度报文、duplicate wait、peer HUP/EOF、非法 fd、adapter 边界下行为正确。
- `ThreadKind::IoUring` 在线程上支持 epoll readiness fallback；io_uring 可用时覆盖文件 `write_at` / `writev_at` / `fsync` / `read_at` / `readv_at`、TCP `accept/connect/recv/send/recvv/sendv` 和 UDP `recvmsg/sendmsg/recvv_from/sendv_to`。

运行：

```sh
ctest --test-dir build-conan/build/Release --output-on-failure
```

## 13. Benchmark

Benchmark 使用 Google Benchmark，入口目标是 `asyncflow_runtime_benchmarks`。

文件布局：

- `benchmarks/io_benchmarks.cpp`：IO adapter inline/zero-byte 快路径开销。
- `benchmarks/queue_benchmarks.cpp`：SPSC、MPSC、对象池基础性能。
- `benchmarks/runtime_benchmarks.cpp`：外部 start、跨线程 hop、parallel shards runtime 路径。

运行：

```sh
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

CI 使用 `benchmarks/perf_baseline_github_ubuntu.json` 作为 GitHub Ubuntu runner 的 runtime benchmark baseline，并由 `scripts/check_benchmark_regression.py` 按阈值检测回归。本地机器仍可使用 `benchmarks/perf_baseline.json` 做开发参考。

## 14. 构建

依赖由 Conan 管理：

```text
gtest/1.17.0
benchmark/1.9.5
```

CMake 使用现代 target 方式：

- `asyncflow`：header-only interface library。
- `AsyncFlow::AsyncFlow`：别名 target。
- `ASYNCFLOW_BUILD_EXAMPLES`：是否构建示例。
- `ASYNCFLOW_BUILD_TESTS`：是否构建测试。
- `ASYNCFLOW_BUILD_BENCHMARKS`：是否构建 benchmark。

Release 构建示例：

```sh
conan install . --output-folder=build-conan --build=missing -s build_type=Release
cmake -S . -B build-conan/build/Release \
  -DCMAKE_TOOLCHAIN_FILE=build-conan/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-conan/build/Release --parallel
ctest --test-dir build-conan/build/Release --output-on-failure
```

## 15. 当前语义边界

- `shutdown()` 必须从非 runtime 线程调用；debug 下会断言。
- `StopImmediately` 默认只保证 executor 尽快停止；如需完整 pending task 释放，应开启 `enable_task_registry`。
- 有序 batch 的连续性由 `last_applied_batch_id` 保护；失败后的重试、跳过、补偿策略由业务实现，可使用 `OrderedBatchRetrySkipPolicy` 辅助决策。
- `parallel_shards()` 必须在 runtime 线程中由 owner task 调用。
- enum 必须连续递增，真实线程索引从 0 开始。
- 业务跨线程传递应优先传 id、值对象或不可变数据，不应跨 owner thread 直接传可变业务对象引用。
- `ThreadKind::IoUring` 依赖 Linux 内核和容器权限；不可用时 `io_uring_backend_available()` 返回 false，线程仍可作为 epoll readiness IO 线程使用，TCP/UDP helper 也会自动退回 readiness 路径。

## 16. CI 与后续可选增强

当前 CI 覆盖普通测试、TSAN stress 和 runtime benchmark 回归检查：

- `.github/workflows/ci.yml`：Debug 测试、TSAN stress、Release benchmark 三个 job。
- `tests/runtime_stress_tests.cpp`：高并发反复 `init()` / `shutdown()` / `start_task()`，默认短跑，可通过 `ASYNCFLOW_STRESS_MS` 拉长。
- `benchmarks/perf_baseline.json`：本地 runtime benchmark baseline。
- `benchmarks/perf_baseline_github_ubuntu.json`：GitHub Ubuntu runner runtime benchmark baseline。
- `scripts/check_benchmark_regression.py`：读取 Google Benchmark JSON，并按 `default_max_regression` 或单项阈值失败。

后续仍可按业务压力继续补充：

- 在稳定 CI 机器上定期刷新 benchmark baseline。
- 为更多业务域补模板，例如 DB 回写、跨服消息 apply。
