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
- io_uring submit 在 executor tick 内合并，多个 SQE 尽量一次 `io_uring_enter` 提交；SQ 接近阈值或线程准备阻塞前会强制 flush，兼顾吞吐和尾延迟。
- `af::io_openat()` / `io_openat2()` / `io_mkdirat()` / `io_close()` / `io_statx()` / `io_fallocate()` / `io_ftruncate()` / `io_linkat()` / `io_symlinkat()` / `io_renameat()` / `io_unlinkat()` 和 `af::IoFile::read_at()` / `write_at()` / `readv_at()` / `writev_at()` / `fsync()` 通过 io_uring 提交真正的文件生命周期操作，completion 后恢复原 task。
- `af::io_sendfile_some()` 通过 Linux `sendfile(2)` 做文件到 socket 的内核态搬运，遇到 socket buffer 满时等待 out fd writable；`af::io_splice_some()` 在 `ThreadKind::IoUring` 优先提交 `IORING_OP_SPLICE`，不可用时退回 `splice(2)` + readiness。
- `af::io_send_zc_some()` / `af::TcpStream::send_zc_some()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_SEND_ZC`，runtime 通过 probe 避免在不支持的内核上反复提交失败；主 CQE 恢复业务 task，notification CQE 只用于释放内部 operation，避免复用 `IoOpState` 时出现悬挂写入。
- `af::TcpListener::accept_some()` / `af::TcpStream::connect()` / `recv_some()` / `send_some()` / `send_zc_some()` / `recvv_some()` / `sendv_some()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT` / `IORING_OP_RECV` / `IORING_OP_SEND` / `IORING_OP_SEND_ZC` 或 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`；`af::UdpSocket::recv_from_some()` / `send_to_some()` / `recvv_from_some()` / `sendv_to_some()` 优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，ring 不可用或 would-block 时退回 epoll readiness。
- `af::IoEvent` 使用 Linux `eventfd` readiness，适合业务侧异步通知、轻量计数器和跨组件唤醒，`ThreadKind::IoUring` 线程也可复用 epoll fallback。
- `af::IoTimer` 使用 Linux `timerfd` readiness，适合超时、重试、心跳和连接保活，`ThreadKind::IoUring` 线程也可复用 epoll fallback。
- `af::IoFile` / `af::TcpListener` / `af::TcpStream` / `af::UdpSocket` / `af::IoEvent` / `af::IoTimer` 仅保存 `thread + fd`，内联转发到 IO helper，不拥有 fd、不分配堆内存、不增加额外分支表；`af::TcpStream::sendfile_some()` 只是 thin adapter，不接管文件 fd 或 socket fd 所有权。

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

### 11.8 io_uring_openat.cpp：异步 openat 文件业务模板

文件：`examples/io_uring_openat.cpp`

该示例展示：

- `af::io_openat()` 在指定 `ThreadKind::IoUring` 线程上提交 `IORING_OP_OPENAT`。
- path 保存为 task 成员，保证 pending open 恢复前仍然有效。
- 打开 fd 后继续使用 `af::IoFile::write_at()` / `fsync()` / `read_at()` 完成文件 round trip。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_openat_example
```

### 11.9 io_uring_file_lifecycle.cpp：文件生命周期业务模板

文件：`examples/io_uring_file_lifecycle.cpp`

该示例展示：

- `af::io_openat()` / `io_fallocate()` / `io_statx()` / `io_renameat()` / `io_unlinkat()` / `io_close()` 在指定 IO 线程上完成文件生命周期操作。
- `io_close()` 接收 `af::UniqueFd&`，提交成功后立即 release，避免 pending close 期间重复关闭 fd。
- 文件 path 保存为 task 成员，保证 pending rename/unlink/statx 恢复前仍然有效。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_file_lifecycle_example
```

### 11.10 io_uring_filesystem_ops.cpp：目录和文件生命周期业务模板

文件：`examples/io_uring_filesystem_ops.cpp`

该示例展示：

- `af::io_openat2()` / `io_mkdirat()` / `io_ftruncate()` / `io_linkat()` / `io_symlinkat()` / `io_unlinkat()` 在指定 IO 线程上完成目录和文件操作。
- 新增 filesystem helper 通过独立 `af/io_filesystem.hpp` 暴露，公共 API 仍由 `af/io.hpp` 引入，runtime 侧使用窄 SQE submit 路径减少无关分支。
- 示例 task 的每个状态拆成成员函数，完成等待交给 `ShutdownPolicy::WaitForTasks` 下的 `runtime::shutdown()`；需要在 shutdown 前继续派发子任务的示例先用 `runtime::wait_for_idle()` 自然清空，避免示例层显式定义 atomic 只为了判断 task 是否完成。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_filesystem_ops_example
```

### 11.10 io_sendfile_static.cpp：TCP 静态文件少拷贝发送模板

文件：`examples/io_sendfile_static.cpp`

该示例展示：

- `af::TcpStream::sendfile_some()` 在指定 IO 线程上通过 `sendfile(2)` 把文件内容发送到 TCP peer。
- sendfile offset 保存为 task 成员；offset 为 `nullptr` 时使用文件当前偏移，业务需要自己保证文件 offset 正确。
- socket buffer 满时 helper 返回 pending，runtime 等待 out fd writable 后恢复同一个 task 继续推进。

运行：

```sh
./build-conan/build/Release/asyncflow_io_sendfile_static_example
```

### 11.11 io_uring_send_zc.cpp：TCP 发送侧 zero-copy 模板

文件：`examples/io_uring_send_zc.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::TcpStream::send_zc_some()` 发送 TCP payload。
- 内核支持 `IORING_OP_SEND_ZC` 时走 io_uring zero-copy send；不支持或 socket 返回不支持时退回普通非阻塞 `send` + readiness。
- 发送状态保存 offset 和 `IoOpState` 为 task 成员，每个状态拆成成员函数。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_send_zc_example
```

### 11.12 io_uring_datagram.cpp：UDP io_uring 业务模板

文件：`examples/io_uring_datagram.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::UdpSocket` 完成 client/server round trip。
- `recv_from_some()` / `send_to_some()` 优先走 `recvmsg/sendmsg` completion，不可用时自动退回 epoll readiness。
- 每个状态拆成成员函数，避免把业务流程揉在一个 `switch` 分支里。

运行：

```sh
./build-conan/build/Release/asyncflow_io_uring_datagram_example
```

### 11.13 io_tcp_connect_accept.cpp：TCP accept/connect 业务模板

文件：`examples/io_tcp_connect_accept.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::TcpListener` 和 `af::TcpStream` 完成 TCP server/client round trip。
- `accept_some()` / `connect()` 优先走 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT`，不可用时自动退回 epoll readiness。
- server/client 每个状态拆成成员函数，覆盖 accept、connect、send 和 recv 的常见业务骨架。

运行：

```sh
./build-conan/build/Release/asyncflow_io_tcp_connect_accept_example
```

### 11.14 io_vectored.cpp：scatter/gather stream/datagram 业务模板

文件：`examples/io_vectored.cpp`

该示例展示：

- `ThreadKind::IoUring` 线程上用 `af::TcpStream::sendv_some()` / `recvv_some()` 完成两段 buffer 的 stream round trip，并用 `af::UdpSocket::sendv_to_some()` / `recvv_from_some()` 完成 datagram round trip。
- 协议头、正文或日志片段可以直接作为 `iovec` 数组发送，避免先拼到连续临时 buffer。
- `iovec` 数组和其指向的 buffer 在 pending IO 完成前必须保持有效，因此示例将它们保存为 task 成员。

运行：

```sh
./build-conan/build/Release/asyncflow_io_vectored_example
```

### 11.15 io_timer.cpp：timerfd 异步定时器业务模板

文件：`examples/io_timer.cpp`

该示例展示：

- `af::make_timerfd()` 创建非阻塞 timer fd，fd 生命周期仍由 `af::UniqueFd` 管理。
- `af::IoTimer::wait()` 在绑定 IO 线程上等待 timerfd readable，并在到期后恢复原 task。
- `af::arm_timerfd_after()` 可用于超时、重试、心跳和连接保活。

运行：

```sh
./build-conan/build/Release/asyncflow_io_timer_example
```

### 11.16 io_event.cpp：eventfd 异步通知业务模板

文件：`examples/io_event.cpp`

该示例展示：

- `af::make_eventfd()` 创建非阻塞 event fd，fd 生命周期仍由 `af::UniqueFd` 管理。
- `af::IoEvent::wait()` 在绑定 IO 线程上等待 eventfd readable，并在收到通知后恢复原 task。
- `af::write_eventfd()` 可用于业务侧异步通知、轻量计数器和跨组件唤醒。

运行：

```sh
./build-conan/build/Release/asyncflow_io_event_example
```

## 12. 测试覆盖

测试使用 GTest，入口目标是 `asyncflow_runtime_tests`。

文件布局：

- `tests/runtime_lifecycle_tests.cpp`：任务生命周期、状态机、背压、shutdown 策略。
- `tests/runtime_parallel_tests.cpp`：parallel shards、失败汇总、有序 batch、ordered start 边界、retryable ordered apply。
- `tests/runtime_stress_tests.cpp`：高并发 init/shutdown/start_task stress，可配合 TSAN 拉长运行。
- `tests/utility_tests.cpp`：SPSC/MPSC/MPMC 队列、对象池、分片工具、CRUD helper、BatchSequencer、ordered retry/skip policy。
- `tests/runtime_io_*_tests.cpp`：按 setup、epoll、stream/zero-copy、io_uring socket、io_uring file、datagram、shutdown 拆分 IO 覆盖；公共 fixture 和 task helper 放在 `tests/runtime_io_test_support.hpp`。

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
- epoll IO task 在 readable/writable、eventfd、timerfd、UDP 零长度报文、duplicate wait、peer HUP/EOF、非法 fd、adapter 边界下行为正确。
- `ThreadKind::IoUring` 在线程上支持 epoll readiness fallback；io_uring 可用时覆盖文件 `openat/close/statx/fallocate/renameat/unlinkat/write_at/writev_at/fsync/read_at/readv_at`、TCP `accept/connect/recv/send/send_zc/recvv/sendv` 和 UDP `recvmsg/sendmsg/recvv_from/sendv_to`。

运行：

```sh
ctest --test-dir build-conan/build/Release --output-on-failure
```

## 13. Benchmark

Benchmark 使用 Google Benchmark，入口目标是 `asyncflow_runtime_benchmarks`。

文件布局：

- `benchmarks/io_*_benchmarks.cpp`：按 adapter、filesystem、zero-copy、file/fixed-file、vectored 拆分 IO 快路径压测；公共 fake runtime 放在 `benchmarks/io_benchmark_support.hpp`。
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

## 16. 核心框架性能与模块化检查记录

### 16.1 固定线程异步任务核心检查结论

已检查核心固定线程任务框架的任务生命周期、固定线程调度、队列、对象池、shutdown 边界和 cacheline 布局。当前未发现 P0/P1 级逻辑错误、明显内存泄漏、use-after-free 或数据竞争问题。

已验证：

- Docker GCC Debug 核心 runtime/queue/pool/stress 测试通过。
- 本地 TSAN 核心 lifecycle/shutdown/parallel/queue/pool/stress 测试通过；IO 环境相关项按环境 skip。
- Release runtime benchmark 通过 GitHub Ubuntu baseline 回归检查。

当前热路径设计：

- 固定 runtime 线程之间使用 source -> target 一条 bounded SPSC ring。
- runtime 线程调度到自身时走 executor 本地无锁 ring，避免同线程投递也走跨线程队列。
- 非 runtime 线程进入 executor 使用 bounded MPSC ingress。
- 每个 task type 使用独立对象池，slot 按 cache line 对齐，并有 TLS 小缓存减少频繁回到共享 free queue。
- `WaitForTasks` 通过 unfinished task counter 等已接收任务结束；`StopImmediately` 可用 task registry 取消并释放 pending/queued task。

### 16.2 已记录性能待整改项

P2：`Task::run()` 是虚函数间接调用。当前模型简洁稳定，但极小粒度任务在千万级调度下会有 indirect branch 和 i-cache 成本。可评估 CRTP/static trampoline 或 type-erased function pointer，把动态分派从每次 run 降到创建时绑定。

P2：跨线程投递后的 wake 检查仍可减少共享 cacheline 访问。当前 enqueue 后会 `mark_source_ready()` 再 `notify()`；批量投递时可让 `mark_ready()` 返回 ready bit 是否从 0 变 1，只在首次变 ready 时触发 wake 检查，减少 `sleeping_` cacheline 读/CAS。

P2：SPSC 队列矩阵目前是 `vector<unique_ptr<SpscQueue>>`，热路径取队列有一层 pointer chase。可改成连续矩阵存储或定制 arena，减少间接寻址并改善预取/TLB locality。

P2：`thread_count > 64` 时 ready source 退化为全扫描。64 线程以内使用 bitmask 很高效；如果业务未来需要超过 64 个固定线程，应改成多 word ready bitmap。

P3：`start_task()` 为通用 handle 生命周期多做一次引用计数增减。可新增无 handle 的 fast path，仅用于立即启动且不暴露 handle 的场景，减少外部投递热路径原子操作。

### 16.3 模块化拆分评估

当前 `include/af/async_runtime.hpp` 承担了过多职责：public runtime facade、task handle、parallel shards、有序 batch、executor run loop、队列调度、任务 registry、epoll readiness、io_uring setup/SQE submit/completion/cancel、IO fallback 和 shutdown 逻辑都在同一文件中。逻辑高内聚于 runtime，但文件过长，阅读和局部修改成本偏高。

可以拆，而且不需要牺牲性能，前提是保持 header-only、模板可见、热路径函数仍在头文件内联展开。推荐不要用普通 `.cpp` 编译单元隐藏实现，否则会损失模板特化和内联机会。

建议拆分边界：

- `include/af/detail/runtime_common.hpp`：`RuntimeStatus`、`CacheLineAtomic`、小型 traits 派生常量、通用 helper。
- `include/af/detail/runtime_lifecycle.hpp`：task allocate/destroy、handle release、unfinished counter、task registry、shutdown 边界。
- `include/af/detail/runtime_dispatch.hpp`：SPSC/MPSC/local queue 投递、ready bitmap、wake/notify、`post()`/`post_blocking()`。
- `include/af/detail/runtime_executor.hpp`：executor 成员、run loop、`execute()`、`finish_done/pending/again()`。
- `include/af/detail/runtime_parallel.hpp`：parallel shards、ordered batch、ordered start state。
- `include/af/detail/runtime_io_epoll.hpp`：epoll readiness wait/cancel/wake、eventfd drain、deferred delete。
- `include/af/detail/runtime_io_uring.hpp`：io_uring setup/teardown、SQ/CQ ring、operation pool、completion、cancel。
- `include/af/detail/runtime_io_submit.hpp`：public `io_submit_*` thin forwarding 与分支较多的 SQE builder。后续可继续拆为 socket/file/datagram/filesystem submit helper。

拆分难点：

- `Executor` 当前是 `AsyncRuntime<Traits>` 的 nested class，直接访问大量 private static 状态。低风险拆法不是简单把文本搬到另一个文件，而是先把 helper 下沉到 `detail` 模板类，例如 `RuntimeExecutor<RuntimeT>`、`RuntimeTaskLifecycle<RuntimeT>`、`RuntimeIoUring<RuntimeT>`，再由 `AsyncRuntime` 组合/转发。
- IO submit API 数量多，直接大拆容易制造签名漂移。优先拆纯内部 helper，不先改 public API。
- hot path 需要持续 benchmark 守护。每个拆分阶段都要跑 runtime benchmark baseline，避免模块化过程中引入额外 indirection。

建议执行顺序：

1. 先拆无行为变化的 common/parallel/lifecycle helper，保持 `async_runtime.hpp` 作为 facade。
2. 再拆 dispatch/executor，并同步加入 wake 去重优化的 benchmark 对照。
3. 最后拆 IO epoll/io_uring，因为这部分状态多、边界复杂，适合在核心调度稳定后单独提交。
4. 每阶段都运行 `git diff --check`、Debug `ctest`、TSAN 核心测试、Release runtime benchmark 回归检查。

### 16.4 模块化复查补充

本次复查按文件行数和职责边界确认，`include/af/async_runtime.hpp` 约 7900 行，是当前最主要的可维护性风险点。测试侧 `tests/runtime_io_test_support.hpp` 约 6500 行，也已经承担了过多 fixture、task helper、socket/file/datagram/timer/event/uring 边界任务，应和 runtime 拆分同步治理，避免核心代码变清晰但测试支持文件继续膨胀。

`async_runtime.hpp` 当前仍适合作为 public facade 和模板入口，但不适合继续承载全部实现细节。建议保持 header-only，不把热路径挪进 `.cpp`，以免破坏模板实例化、`if constexpr` 裁剪和编译期内联。实现可拆成 detail fragment 或 `detail::Runtime*<RuntimeT>` helper，两种方式都不增加运行时虚调用；初期优先使用低风险 include fragment，后续再把稳定边界提升为真正 helper 类型。

优先拆分点：

- common：`RuntimeStatus`、`CacheLineAtomic`、`OrderedBatchState`、`ExternalPostCounter` 等通用小类型，行为独立，适合第一步移动。
- parallel：`ParallelGroup`、`ShardTask`、ordered guard、ordered start state，和 executor/IO 的耦合较低，适合第二步移动。
- lifecycle：task pool、handle release、registry、unfinished counter、StopImmediately cancel，边界清晰，但必须保持任务引用计数和 registry 顺序不变。
- dispatch：local queue、SPSC/MPSC ingress、ready bitmap、wake/notify，这里是核心热路径，拆分时应同时评估 wake 去重和 SPSC 连续矩阵存储，必须以 benchmark 守护。
- executor：run loop、execute/finish、sleep/wake 逻辑依赖内部状态多，适合在 common/parallel/lifecycle 拆完后单独提交。
- IO：epoll readiness、io_uring ring、SQE builder、operation pool、cancel/timeout/fallback 目前耦合最重，最后拆；拆分时应按 `epoll`、`io_uring setup/completion`、`file submit`、`socket submit`、`datagram submit` 分层。

测试、示例、benchmark 也需要按同一原则整理：

- `tests/runtime_io_test_support.hpp` 拆为 `runtime_io_fixture.hpp`、`runtime_io_file_tasks.hpp`、`runtime_io_socket_tasks.hpp`、`runtime_io_datagram_tasks.hpp`、`runtime_io_timer_event_tasks.hpp`、`runtime_io_uring_tasks.hpp`。
- IO benchmark 已按 adapter/file/filesystem/vectored/zero-copy 分文件，当前结构基本合理；后续新增 benchmark 继续按功能分组，不再回到单一大文件。
- 示例文件目前大多按业务模板独立，重点是保持每个 task 的 `run()` 状态分派调用成员函数，不把业务状态机继续堆进单个 `switch` 分支。

拆分验收标准：

- public API 不漂移，`async_runtime.hpp` 仍是用户主要 include 入口。
- 热路径不增加额外堆分配、虚调用或不可内联跳转。
- 每个拆分提交只移动一个清晰职责块，并跑 `git diff --check`、Debug 关键测试、TSAN 核心测试和 Release runtime benchmark 回归检查。
- 每次拆分后记录文件行数变化，确保大文件确实变短，而不是把复杂度复制到新位置。

## 17. CI 与后续可选增强

当前 CI 覆盖普通测试、TSAN stress 和 runtime benchmark 回归检查：

- `.github/workflows/ci.yml`：Debug 测试、TSAN stress、Release benchmark 三个 job。
- `tests/runtime_stress_tests.cpp`：高并发反复 `init()` / `shutdown()` / `start_task()`，默认短跑，可通过 `ASYNCFLOW_STRESS_MS` 拉长。
- `benchmarks/perf_baseline.json`：本地 runtime benchmark baseline。
- `benchmarks/perf_baseline_github_ubuntu.json`：GitHub Ubuntu runner runtime benchmark baseline。
- `scripts/check_benchmark_regression.py`：读取 Google Benchmark JSON，并按 `default_max_regression` 或单项阈值失败。

后续仍可按业务压力继续补充：

- 在稳定 CI 机器上定期刷新 benchmark baseline。
- 为更多业务域补模板，例如 DB 回写、跨服消息 apply。
