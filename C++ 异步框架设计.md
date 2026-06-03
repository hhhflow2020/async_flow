# AsyncFlow C++ 异步框架需求与设计

本文档描述 AsyncFlow 的需求、设计实现、公开 API、性能边界、测试与示例。当前实现是一个基于 C++20 的固定线程异步任务框架，框架名为 `AsyncFlow`，命名空间为 `af`。

## 1. 目标

AsyncFlow 不是通用线程池，而是固定 Executor/EventLoop 模型：

```text
AsyncRuntime + Fixed Threads + Task + Scheduler + Shard + Ordered Batch
```

核心目标：

- 业务线程在编译期通过 `thread_layout` 声明线程组、线程数量、线程能力和调试名称。
- 任务对象由框架创建、调度、释放，业务不直接 `new` / `delete` / 栈上创建任务。
- 任务内部可写状态机，可跨固定线程挂起恢复。
- 同一个业务 key 始终路由到同一个逻辑线程，减少共享数据加锁。
- 支持 bounded 队列，不能无限积压任务。
- 固定线程间使用 SPSC 队列，外部入口使用 bounded MPSC 队列。
- 支持无序 shard 批处理，只调度非空 shard。
- 支持有序 batch，全 shard 都推进 `last_applied_batch_id`。
- 使用模板静态绑定 handler 和任务类型，减少虚函数之外的间接调用、分支和动态分配。
- 使用现代 CMake + Conan 管理构建、测试和 benchmark。

网络层目标补充：

- 新 `af::net` 主路径采用 reactor-driven 设计，IO 线程直接管理 fd、事件、
  buffer 和连接状态，不再把每个 socket readiness 绑定到普通 task 的
  pending/resume 热路径。
- Linux native readiness 主路径采用 epoll LT，事件触发后 drain 到 `EAGAIN` 或预算耗尽；
  `epoll_ctl` 只发生在 fd 注册、interest 变化和关闭时；`ThreadKind::IoUring`
  线程优先用 io_uring poll 承接同一套 `NetIoChannel` readiness，并按
  level-multishot、multishot、one-shot 的顺序自适应降级，最后才回退 epoll LT。
- `TcpServer`、`UdpServer`、Unix socket server/client 都是绑定框架 IO 线程的网络服务
  抽象；业务 task 只处理业务计算和跨线程流程。
- `TcpConnectionHandle` 由 `io_thread + slot + generation` 组成，允许业务层安全地
  管理 `user_id -> connection`，并防止 fd/slot 复用导致旧任务误发。
- TCP 核心 API 只提供字节流 `on_read`，UDP 提供 `on_datagram`；packet id、包长度、
  protobuf/json 解析和分发由用户业务层决定。框架可提供可选 parser/router 工具，
  但不强制 codec/dispatcher 进入核心。
- protobuf 仅用于示例 `tcp_login_server`，不进入 `af::net` 核心库依赖。
- 旧 task-driven IO examples/tests/benchmarks 已从本分支移除；底层 `io_*` helper
  作为日志、timer、filesystem 等内部兼容层暂时保留，后续逐步替换到 reactor/channel
  抽象。

## 2. 线程定义

业务使用 tag + `thread_layout` 定义固定线程。不再要求业务手写连续递增 enum；runtime 会根据 layout 在编译期生成连续线程索引，并保留线程组的 begin/count/at()/shard() 视图。

```cpp
struct AppLogicThreadTag;
struct AppDbThreadTag;
struct AppIoThreadTag;

struct AppRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<AppLogicThreadTag, 4, af::ThreadKind::Worker, "logic">(),
                          af::thread_group<AppDbThreadTag, 1, af::ThreadKind::Worker, "db">(),
                          af::thread_group<AppIoThreadTag, 1, af::ThreadKind::Epoll, "io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
using AppThread = async::Thread;

inline constexpr auto player_logic_threads = async::thread_group<AppLogicThreadTag>();
inline constexpr auto app_db_threads = async::thread_group<AppDbThreadTag>();
inline constexpr auto app_io_threads = async::thread_group<AppIoThreadTag>();
inline constexpr AppThread player_logic_begin = player_logic_threads.begin();
inline constexpr std::uint16_t player_logic_shard_count = player_logic_threads.count;

struct AppThreads {
    static constexpr AppThread Logic_0 = player_logic_threads.template at<0>();
    static constexpr AppThread Logic_1 = player_logic_threads.template at<1>();
    static constexpr AppThread Logic_2 = player_logic_threads.template at<2>();
    static constexpr AppThread Logic_3 = player_logic_threads.template at<3>();
    static constexpr AppThread DB_0 = app_db_threads.template at<0>();
    static constexpr AppThread IO_0 = app_io_threads.template at<0>();
};

inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return player_logic_threads.shard(player_id);
}
```

规则：

- `thread_layout(...)` 至少包含一个线程组。
- 每个 `thread_group<Tag, Count, Kind, Name>()` 的 tag 必须唯一，`Count` 必须大于 0。
- `Kind` 声明线程能力，例如 `Worker`、`Io`、`Epoll`、`IoUring`、`Log`。
- `Name` 用于 runtime 启动时调用系统线程命名接口，线程名形如 `af-logic-0`、`af-io-0`，方便 `top`、`ps -L`、`htop`、`lldb/gdb` 定位。
- `thread_group<Tag>()` 返回编译期轻量视图，`begin()`、`count`、`at<N>()`、`at(index)`、`shard(key)` 都只做整数运算，不引入堆分配。
- `async::Thread` 本质上保存一个 `uint16_t` index；普通调度热路径直接用该 index 定位 executor 和队列。

## 3. Runtime Traits

`af::AsyncRuntime<Traits>` 只消费框架层参数。

必填项：

```cpp
static constexpr auto threads = af::thread_layout(...);
```

可选项：

```cpp
static constexpr std::size_t spsc_queue_capacity = 1024;
static constexpr std::size_t external_queue_capacity = 1024;
static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
```

默认值：

- `spsc_queue_capacity = 1024`
- `external_queue_capacity = 1024`
- `runtime_queue_full_policy = QueueFullPolicy::Reject`
- `external_queue_full_policy = QueueFullPolicy::Reject`
- `shutdown_policy = ShutdownPolicy::WaitForTasks`

旧的单一 `queue_full_policy` 已移除。runtime 内部生产者和外部生产者必须分别通过 `runtime_queue_full_policy` / `external_queue_full_policy` 声明满队列策略，避免一个总开关隐式影响两条语义不同的热路径。

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
bool schedule(Thread thread, af::ScheduleMode mode = af::ScheduleMode::Auto) noexcept;
bool schedule_fast(Thread thread) noexcept;
bool schedule_ordered(Thread thread) noexcept;

TaskResult pending_on(Thread thread, af::ScheduleMode mode = af::ScheduleMode::Auto) noexcept;
TaskResult pending_fast(Thread thread) noexcept;
TaskResult pending_ordered(Thread thread) noexcept;

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
- 默认 `schedule()` / `pending_on()` 使用 `ScheduleMode::Auto`，runtime 线程优先走 local/SPSC 低开销路径。
- 需要明确低开销 runtime-thread-only 路径时使用 `schedule_fast()` / `pending_fast()`。
- 需要多个生产者在目标线程上共享统一入队顺序时使用 `schedule_ordered()` / `pending_ordered()`，该模式会强制走目标 MPSC。
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
bool ok = async::post(AppThreads::Logic_0, task);
bool ordered = async::post(AppThreads::Logic_0, task, af::ScheduleMode::Ordered);
AppThread thread = async::current_thread();
std::uint16_t index = async::current_thread_index();
bool runtime_thread = async::is_runtime_thread();
bool stopping = async::is_stopping();
```

线程索引转换：

```cpp
std::uint16_t index = async::thread_index(AppThreads::Logic_0);
AppThread thread = async::thread_from_index(index);
auto logic_threads = async::thread_group<AppLogicThreadTag>();
```

分片：

```cpp
AppThread thread = player_logic_threads.shard(player_id);
auto sharded = async::split_by_shard(std::move(ops), shard_count, key_fn);
```

运行期观测：

```cpp
std::uint32_t count = async::unfinished_task_count();
std::uint64_t batch_id = async::ordered_last_applied_batch_id(AppThreads::Logic_0);
```

## 6. 调度实现

### 6.1 固定线程

`AsyncRuntime` 在 `init()` 时创建 `thread_count` 个 executor。每个 executor 绑定一个固定线程索引，并在该线程内循环执行任务。

### 6.2 队列结构

每个 executor 有三类入队路径：

- executor 本地 bounded queue：当前 runtime 线程投递给自身目标时的最低开销路径。
- `source -> target` bounded SPSC ring：runtime 线程投递给其他 runtime 线程时的一对一路径。
- target bounded MPSC ingress queue：外部线程进入 runtime，或者 runtime 线程显式要求目标顺序时使用。

SPSC 队列使用 head/tail cache 减少共享 cache line 读取；队列容量会向上取 power-of-two，使用 mask 替代取模。

### 6.3 调度模式

```cpp
enum class ScheduleMode : std::uint8_t {
    Auto,
    Fast,
    Ordered,
};
```

`ScheduleMode::Auto` 是默认模式：

- runtime 线程投递到自身目标：走 executor 本地 queue。
- runtime 线程投递到其他 runtime 线程：走 source -> target SPSC。
- 外部线程投递到 runtime：走目标 MPSC。

`ScheduleMode::Fast` 是 runtime-thread-only 的低开销路径：

- runtime 线程投递到自身目标：走 executor 本地 queue。
- runtime 线程投递到其他 runtime 线程：走 source -> target SPSC。
- 外部线程调用会失败，不会隐式 fallback 到 MPSC。

`ScheduleMode::Ordered` 是目标线程统一入队顺序路径：

- runtime 线程投递到自身目标：强制走目标 MPSC。
- runtime 线程投递到其他 runtime 线程：强制走目标 MPSC。
- 外部线程投递到 runtime：走目标 MPSC。

因此，runtime 内部线程投递自身目标时也可以显式选择：

- `schedule_fast(thread)` / `pending_fast(thread)`：保持 local queue 快速路径。
- `schedule_ordered(thread)` / `pending_ordered(thread)`：强制走 MPSC，与其他生产者共享目标线程 admission order。

完整说明见 `docs/runtime_scheduling_semantics.md`。

### 6.4 满队列策略

```cpp
enum class QueueFullPolicy : std::uint8_t {
    Reject,
    Yield,
};
```

- `Reject`：队列满时调度失败，`schedule()` / `start_task()` 返回 `false`，框架回滚任务状态并释放未启动任务。
- `Yield`：队列满时让出 CPU 并等待空位，适合业务必须接收任务但可以接受短暂等待的入口。

traits 可以用 `runtime_queue_full_policy` 和 `external_queue_full_policy` 分别配置 runtime 生产者和外部生产者。这样可以让 runtime 内部路径在必要时等待，而外部入口保持失败返回，避免外部生产者在满载时被拖住。

本地队列阻塞入队时，如果目标就是当前 executor，会优先执行本地已有任务，避免固定容量队列满后等待自己。

### 6.5 active post 与 shutdown

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
    AppThreads::Logic_0,
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
- `ThreadKind::IoUring` 优先初始化 io_uring，并通过 eventfd 唤醒 completion；`af::net` 的 `NetIoChannel` readiness 在内核支持时优先提交 `IORING_OP_POLL_ADD`，先尝试 multishot level poll，再按内核能力降到 multishot 或 one-shot poll，completion 直接回调 owner IO 线程上的 channel。如果 io_uring 不可用、ring 满、poll add 全部不可用或 submit 失败，线程仍保留 epoll LT fallback。
- io_uring submit 在 executor tick 内合并，多个 SQE 尽量一次 `io_uring_enter` 提交；SQ 接近阈值或线程准备阻塞前会强制 flush，兼顾吞吐和尾延迟。
- `af::io_openat()` / `io_openat2()` / `io_mkdirat()` / `io_close()` / `io_statx()` / `io_fallocate()` / `io_ftruncate()` / `io_linkat()` / `io_symlinkat()` / `io_renameat()` / `io_unlinkat()` 和 `af::IoFile::read_at()` / `write_at()` / `readv_at()` / `writev_at()` / `fsync()` 通过 io_uring 提交真正的文件生命周期操作，completion 后恢复原 task。
- `af::io_sendfile_some()` 通过 Linux `sendfile(2)` 做文件到 socket 的内核态搬运，遇到 socket buffer 满时等待 out fd writable；`af::io_splice_some()` 在 `ThreadKind::IoUring` 优先提交 `IORING_OP_SPLICE`，不可用时退回 `splice(2)` + readiness。
- `af::io_send_zc_some()` / `af::TcpStream::send_zc_some()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_SEND_ZC`，runtime 通过 probe 避免在不支持的内核上反复提交失败；主 CQE 恢复业务 task，notification CQE 只用于释放内部 operation，避免复用 `IoOpState` 时出现悬挂写入。
- `af::TcpListener::accept_some()` / `af::TcpStream::connect()` / `recv_some()` / `send_some()` / `send_zc_some()` / `recvv_some()` / `sendv_some()` 在 `ThreadKind::IoUring` 线程上优先提交 `IORING_OP_ACCEPT` / `IORING_OP_CONNECT` / `IORING_OP_RECV` / `IORING_OP_SEND` / `IORING_OP_SEND_ZC` 或 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`；`af::UdpSocket::recv_from_some()` / `send_to_some()` / `recvv_from_some()` / `sendv_to_some()` 优先提交 `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`，ring 不可用或 would-block 时退回 epoll readiness。
- `af::IoEvent` 使用 Linux `eventfd` readiness，适合业务侧异步通知、轻量计数器和跨组件唤醒，`ThreadKind::IoUring` 线程也可复用 epoll fallback。
- `af::IoTimer` 使用 Linux `timerfd` readiness，适合超时、重试、心跳和连接保活，`ThreadKind::IoUring` 线程也可复用 epoll fallback。
- `af::IoFile` / `af::TcpListener` / `af::TcpStream` / `af::UdpSocket` / `af::IoEvent` / `af::IoTimer` 仅保存 `thread + fd`，内联转发到 IO helper，不拥有 fd、不分配堆内存、不增加额外分支表；`af::TcpStream::sendfile_some()` 只是 thin adapter，不接管文件 fd 或 socket fd 所有权。
- 异步日志提供 `LogOrdering::Ordered` 与 `LogOrdering::Relaxed` 两种生产者队列策略。用户可用 `AsyncLogConfig::ordered()` / `AsyncLogConfig::ordered(producer_shard_count)` / `AsyncLogConfig::relaxed()` / `AsyncLogConfig::relaxed(runtime_thread_count, external_shard_count)` 直接创建推荐策略配置，也可在已有配置上调用 `use_ordered(producer_shard_count)` / `use_relaxed(runtime_thread_count, external_shard_count)` 切换策略；`runtime_thread_count == 0` 在 runtime-bound 入口表示自动使用 `RuntimeT::thread_count`。默认 `Ordered` 使用单个 bounded MPSC，让消费者按入队线性化顺序批量写后端；ordered 的 record pool 与 accepted/dropped 计数按 producer shard 分散，避免除 MPSC enqueue 序列点之外的共享 cache line 热点；`Relaxed` 用 runtime 线程 SPSC lane + 外部 sharded MPSC 换取更低争用，但只保证每个 lane/shard 内 FIFO。`runtime_lane_capacity` 只在 `Relaxed` 下控制 runtime SPSC lane 的容量，ordered 场景只需要设置 `queue_capacity`。日志等级过滤使用 Abseil 前端 `SetMinLogLevel()`，低于等级的 `LOG(...) << ...` 不会进入格式化和异步入队路径。

仍需注意：

- `WaitForTasks` 为了 shutdown 可等待，会在任务首次成功调度和最终完成时维护一个全局 unfinished counter。
- 有序 batch 失败后的重试、跳过和补偿策略仍由业务决定；`af::OrderedBatchRetrySkipPolicy` 只负责记录失败次数并给出 Retry / Skip / Stop 决策。
- 未开启 `enable_task_registry` 时，`StopImmediately` 仍保持最小热路径开销，不追踪 pending task。

## 11. 示例

示例统一放在 `examples/`。

### 11.1 app_runtime.hpp：共享运行时配置

文件：`examples/app_runtime.hpp`

展示内容：

- 使用 tag + `thread_layout` 定义固定线程组。
- 使用 `AppRuntimeTraits` 配置 runtime。
- 定义 `using async = af::AsyncRuntime<AppRuntimeTraits>`。
- 计算 `player_logic_shard_count`。
- 使用 `thread_group<Tag>().shard(key)` 实现 player 到 logic shard 的路由。

关键代码：

```cpp
struct AppRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<AppLogicThreadTag, 4, af::ThreadKind::Worker, "logic">(),
                          af::thread_group<AppDbThreadTag, 1, af::ThreadKind::Worker, "db">(),
                          af::thread_group<AppIoThreadTag, 1, af::ThreadKind::Epoll, "io">());
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
using AppThread = async::Thread;

inline constexpr auto player_logic_threads = async::thread_group<AppLogicThreadTag>();
inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return player_logic_threads.shard(player_id);
}
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
    AppThreads::Logic_0,
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

### 11.6 net_tcp_echo_server.cpp：Reactor TCP echo server

文件：`examples/net_tcp_echo_server.cpp`

展示内容：

- 使用 `af::net::TcpServer<Runtime, Handler>` 绑定多个 `af::preferred_io_thread_kind`
  IO 线程；Linux 默认优先 `ThreadKind::IoUring`，macOS/BSD 使用 kqueue，运行期会保留
  native readiness fallback。
- Linux 下每个 IO 线程一个 listener，并通过 `SO_REUSEPORT` 绑定同一地址，连接从 accept
  开始归属对应 owner IO 线程；macOS/kqueue 路径同样使用 reactor channel 注册 listener
  和 connection fd。
- `on_read(conn, BufferView)` 只处理字节流，示例直接 `conn.send(bytes)` 原样回包。
- 支持 IPv4/IPv6 numeric endpoint；示例默认监听 IPv4，传入 `--ipv6` 时监听 `[::]`。
- 建连和断连日志包含 connection slot / generation，方便观察连接生命周期。
- `SIGINT` / `SIGTERM` 触发停止标记，主线程先调用 `server.stop()`，等待 runtime idle，
  再关闭 runtime；真正的 listener/connection/channel 关闭在 owner IO 线程完成。

运行：

```sh
./build-conan/build/Release/asyncflow_net_tcp_echo_server_example 9090
./build-conan/build/Release/asyncflow_net_tcp_echo_server_example 9090 --ipv6
```

### 11.7 net_tcp_login_server.cpp：长度 + 包 id + protobuf 登录示例

文件：`examples/net_tcp_login_server.cpp`

展示内容：

- TCP 包格式为 `uint32 length + uint16 packet_id + packet_content`，其中 `length` 覆盖 `packet_id + content`。
- 示例层维护 stream parser 和 `packet_id` 分发逻辑，框架核心不强制 codec / dispatcher。
- 登录包 content 使用 `examples/net/login.proto` 中的 `LoginRequest`，protobuf 只作为示例依赖，不进入 `af::net` 核心库。
- IO 线程解析登录包后启动 `LoginTask`，任务切到计算线程打日志并构造 `LoginResponse`，再通过 `TcpConnectionHandle::send()` 回到连接 owner IO 线程发送响应。
- 连接状态用 `slot + generation` 做 key，断连时清理 parser 状态。
- 示例同样使用平台首选 IO 线程并支持 `--ipv6`。

运行：

```sh
./build-conan/build/Release/asyncflow_net_tcp_login_server_example 9091
./build-conan/build/Release/asyncflow_net_tcp_login_server_example 9091 --ipv6
```

## 12. 测试覆盖

测试使用 GTest，入口目标是 `asyncflow_runtime_tests`。

文件布局：

- `tests/runtime_lifecycle_basic_tests.cpp`、`tests/runtime_backpressure_tests.cpp`、`tests/runtime_shutdown_policy_tests.cpp`：任务生命周期、状态机、调度模式、背压和 shutdown 策略。
- `tests/runtime_parallel_shards_tests.cpp`、`tests/runtime_ordered_batch_tests.cpp`、`tests/runtime_ordered_start_tests.cpp`：parallel shards、失败汇总、有序 batch、ordered start 边界、retryable ordered apply。
- `tests/runtime_*_stress_tests.cpp`：高并发 init/shutdown/start_task、cross-thread hop、running->pending、self-post 等 stress，可配合 TSAN 拉长运行。
- `tests/queue_tests.cpp`、`tests/pool_tests.cpp`、`tests/batch_utility_tests.cpp`：SPSC/MPSC/MPMC 队列、对象池、分片工具、CRUD helper、BatchSequencer 和 ordered retry/skip policy。
- `tests/log_tests.cpp`：异步日志格式、runtime-bound consumer、文件/TCP/UDP 后端、ordered/relaxed 队列策略、flush/shutdown、无丢失/无重复和单 producer FIFO 边界。
- `tests/net_buffer_tests.cpp`：`af::Buffer`、`BufferView`、`BufferChain` 基础覆盖。
- `tests/net_socket_address_tests.cpp`：IPv4/IPv6 `TcpEndpoint` 与 `sockaddr_storage` 转换覆盖。
- 旧 task-driven IO tests 已从本分支移除；新的网络服务路径通过 `af::net` 示例、buffer
  tests、socket address tests、本地 macOS kqueue IPv4/IPv6 smoke 和远端 Linux
  preferred-IO smoke 覆盖。

重点覆盖：

- 任务只能通过 runtime factory 创建。
- 未调度任务由 handle 回收。
- 完成任务在 handle 释放后销毁。
- `Done` / `Failed` / `Cancelled` 都能释放任务。
- `pending_on()` 跨线程恢复和 `again()` 当前线程继续。
- Reject 策略下队列满返回失败并销毁被拒绝任务。
- Yield 策略可处理多个外部生产者和同线程 fanout。
- `ScheduleMode::Fast` 拒绝外部生产者，`ScheduleMode::Ordered` 可强制 self-post 走目标 MPSC。
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
- 异步日志默认 ordered 路径跨 batch 保持单 producer FIFO；ordered 与 relaxed 并发 producer 压力下 flush 后无丢失、无重复，并校验 accepted/dropped 计数。
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
- `benchmarks/log_benchmarks.cpp`：压测 runtime-bound `AsyncLogger` 外部 producer 路径，benchmark 每轮校验无 drop、flush 成功、后端 record count 匹配；覆盖 `LogOrdering::Ordered` 与 `LogOrdering::Relaxed`。
- `benchmarks/queue_benchmarks.cpp`：SPSC、MPSC、对象池基础性能。
- `benchmarks/runtime_benchmarks.cpp`：外部 start、跨线程 hop、parallel shards runtime 路径。

运行：

```sh
./build-conan/build/Release/asyncflow_runtime_benchmarks --benchmark_min_time=0.01s
./build-conan/build/Release/asyncflow_runtime_benchmarks \
  --benchmark_filter=AsyncLogger.*ExternalProducers \
  --benchmark_min_time=0.1s \
  --benchmark_repetitions=5 \
  --benchmark_report_aggregates_only=true
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
- 线程必须通过 `thread_layout` 声明；业务不再手写线程 enum，也不直接维护 `thread_count`。
- `Fast` 调度模式只允许 runtime 线程使用；外部线程如果需要进入 runtime，应使用默认 `Auto` 或显式 `Ordered`。
- `Ordered` 调度模式会强制走目标 MPSC，包括 runtime 线程投递给自身目标的场景；不要把它当成本地队列快速路径。
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
- runtime 线程默认调度到自身时走 executor 本地无锁 queue，避免同线程投递也走跨线程队列；如果调用方选择 `ScheduleMode::Ordered`，即使 self-post 也会走目标 MPSC。
- 非 runtime 线程进入 executor 使用 bounded MPSC ingress。
- 异步日志默认使用单全局 MPSC 保持后端可见顺序，同时用 producer shard 分散 record pool 和统计计数；显式 `AsyncLogConfig::relaxed()` 或 `use_relaxed()` 后才使用 runtime SPSC lane 和外部 sharded MPSC。
- 每个 task type 使用独立对象池，slot 按 cache line 对齐，并有 TLS 小缓存减少频繁回到共享 free queue。
- `WaitForTasks` 通过 unfinished task counter 等已接收任务结束；`StopImmediately` 可用 task registry 取消并释放 pending/queued task。

### 16.2 已记录性能待整改项

P2：`Task::run()` 是虚函数间接调用。当前模型简洁稳定，但极小粒度任务在千万级调度下会有 indirect branch 和 i-cache 成本。可评估 CRTP/static trampoline 或 type-erased function pointer，把动态分派从每次 run 降到创建时绑定。

P2：跨线程投递后的 wake 检查仍可减少共享 cacheline 访问。当前 enqueue 后会 `mark_source_ready()` 再 `notify()`；批量投递时可让 `mark_ready()` 返回 ready bit 是否从 0 变 1，只在首次变 ready 时触发 wake 检查，减少 `sleeping_` cacheline 读/CAS。

P2：SPSC 队列矩阵目前是 `vector<unique_ptr<SpscQueue>>`，热路径取队列有一层 pointer chase。可改成连续矩阵存储或定制 arena，减少间接寻址并改善预取/TLB locality。

P3：`start_task()` 为通用 handle 生命周期多做一次引用计数增减。可新增无 handle 的 fast path，仅用于立即启动且不暴露 handle 的场景，减少外部投递热路径原子操作。

### 16.3 模块化现状

当前实现已经完成主要目录化整理：

- `include/af/async_runtime.hpp` 作为 public facade 和模板入口，当前约 345 行。
- `include/af/detail/runtime/` 承载 runtime 配置、生命周期、dispatch、executor、parallel、IO backend 等实现片段。
- `include/af/detail/queue/` 拆分 bounded SPSC/MPSC/MPMC queue family。
- `include/af/detail/io/` 按 common/types/adapters/socket/file/filesystem/datagram/timeout/uring 分目录管理。
- 旧 task-driven IO examples/tests/benchmarks 已移除；新网络层文档见
  `docs/net_reactor_design.md`，当前落地代码集中在 `include/af/net/`、`include/af/buffer/`
  和 `include/af/detail/net/`。TCP server 当前已支持 IPv4/IPv6 endpoint、epoll LT、
  kqueue LT、IoUring 线程上的 native readiness fallback，以及 `close_after_flush()`、
  `shutdown_write()`、`pause_read()`、`resume_read()`、`set_no_delay()`、`set_keepalive()`
  等生产级连接控制 API。

后续如果继续整理文件结构，应遵守以下约束：

- 不把热路径挪进普通 `.cpp`，避免损失模板特化、`if constexpr` 裁剪和内联机会。
- 不把 local/SPSC/MPSC 三条调度路径抽象成一个泛型队列入口；这会掩盖语义并增加性能风险。
- 每次整理只移动一个清晰职责块，并用 `git diff --check`、Debug 关键测试、TSAN/远端 Linux 测试和 Release benchmark 做验证。
- 代码拆分不是默认目标；只有当职责边界、可读性或验证成本确实改善时才做。

## 17. CI 与后续可选增强

当前 CI 覆盖普通测试、TSAN stress 和 runtime benchmark 回归检查：

- `.github/workflows/ci.yml`：Debug 测试、TSAN stress、Release benchmark 三个 job。
- `asyncflow_runtime_stress_tests` 目标覆盖 lifecycle、self-post、cross-thread hop、parallel owner resume 和 running->pending 边界，默认短跑，可通过 `ASYNCFLOW_STRESS_MS` 拉长。
- `benchmarks/perf_baseline.json`：本地 runtime benchmark baseline。
- `benchmarks/perf_baseline_github_ubuntu.json`：GitHub Ubuntu runner runtime benchmark baseline。
- `scripts/check_benchmark_regression.py`：读取 Google Benchmark JSON，并按 `default_max_regression` 或单项阈值失败。

后续仍可按业务压力继续补充：

- 在稳定 CI 机器上定期刷新 benchmark baseline。
- 为更多业务域补模板，例如 DB 回写、跨服消息 apply。
