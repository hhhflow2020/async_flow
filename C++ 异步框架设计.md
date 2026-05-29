C++ 异步任务框架需求

## 1. 设计目标

设计一个轻量级 C++ 异步任务框架，核心目标是：

```cpp
任务对象化
任务可手动调度到指定线程
任务内部支持状态机
任务结束后自动释放
支持按线程分片管理数据，减少加锁
支持一批数据按 shard 并行处理
支持有序 batch 的全 shard 顺序屏障
```

框架不是普通线程池，而是：

```cpp
Executor/EventLoop + Task + Scheduler + Shard
```

---

# 2. 核心使用方式

业务层希望这样启动任务：

```cpp
start_task<LoginTask>(player_id, token);
start_task<AddGoldTask>(player_id, 100);
start_task<ApplyBatchTask>(batch);
```

不希望业务层手写：

```cpp
new Task
Init
Schedule
delete
```

而是统一由框架管理。

---

# 3. Task 基础接口

任务基类保持极简：

```cpp
enum class TaskResult {
    Done,       // 任务完成，框架自动释放
    Pending,    // 任务挂起，等待其他线程 / IO / 子任务完成
    Again       // 继续调度自己执行下一步
};

class Task {
public:
    virtual ~Task() = default;

private:
    virtual TaskResult run() = 0;

    friend class Executor;
};
```

任务执行完成后：

```cpp
return TaskResult::Done;
```

框架自动释放任务。

任务需要等待异步结果时：

```cpp
return TaskResult::Pending;
```

任务想继续执行下一状态时：

```cpp
return TaskResult::Again;
```

---

# 4. 任务启动接口

统一使用：

```cpp
template <typename T, typename... Args>
void start_task(Args&&... args) {
    auto* task = new T();
    task->do_it(std::forward<Args>(args)...);
}
```

业务任务类实现：

```cpp
class AddGoldTask : public Task {
public:
    void do_it(uint64_t player_id, int gold) {
        player_id_ = player_id;
        gold_ = gold;

        Runtime::post(get_player_executor(player_id_), this);
    }

private:
    TaskResult run() override {
        auto& player = PlayerStorage::current().get(player_id_);
        player.add_gold(gold_);

        return TaskResult::Done;
    }

private:
    uint64_t player_id_ = 0;
    int gold_ = 0;
};
```

调用方：

```cpp
start_task<AddGoldTask>(player_id, 100);
```

---

# 5. Executor 接口

每个线程对应一个 Executor。

```cpp
enum class ExecutorType {
    Logic,
    IO,
    DB,
    Compute,
    Timer
};

struct ExecutorId {
    ExecutorType type;
    uint16_t index;
};
```

调度接口：

```cpp
class Runtime {
public:
    static void post(ExecutorId executor, Task* task);
    static ExecutorId current_executor();

    static int logic_shard_count();
};
```

例如：

```cpp
Runtime::post(ExecutorId{ExecutorType::Logic, 3}, task);
```

---

# 6. 数据分片接口

业务数据按 shard 归属某个线程。

```cpp
ExecutorId get_player_executor(uint64_t player_id) {
    return {
        ExecutorType::Logic,
        static_cast<uint16_t>(player_id % Runtime::logic_shard_count())
    };
}
```

核心规则：

```cpp
同一个 player_id 永远由同一个 Logic Executor 处理。
```

业务数据只能在 owner executor 访问：

```cpp
assert(Runtime::current_executor() == get_player_executor(player_id));
```

跨线程不能传：

```cpp
Player*
Room*
可变对象引用
```

跨线程应该传：

```cpp
player_id
room_id
普通值对象
不可变数据
```

---

# 7. 任务状态机需求

任务内部可以维护状态机。

```cpp
class LoginTask : public Task {
public:
    void do_it(uint64_t player_id, std::string token) {
        player_id_ = player_id;
        token_ = std::move(token);
        state_ = State::Start;

        Runtime::post(get_player_executor(player_id_), this);
    }

private:
    enum class State {
        Start,
        QueryDB,
        BackToLogic,
        Finish
    };

    TaskResult run() override {
        switch (state_) {
        case State::Start:
            state_ = State::QueryDB;
            Runtime::post(ExecutorId{ExecutorType::DB, 0}, this);
            return TaskResult::Pending;

        case State::QueryDB:
            // DB 线程处理
            state_ = State::BackToLogic;
            Runtime::post(get_player_executor(player_id_), this);
            return TaskResult::Pending;

        case State::BackToLogic:
            // 回到玩家所属 Logic 线程
            state_ = State::Finish;
            return TaskResult::Again;

        case State::Finish:
            return TaskResult::Done;
        }

        return TaskResult::Done;
    }

private:
    State state_ = State::Start;
    uint64_t player_id_ = 0;
    std::string token_;
};
```

---

# 8. Executor 执行规则

Executor 执行任务后，根据返回值处理：

```cpp
void Executor::execute(Task* task) {
    TaskResult r = task->run();

    switch (r) {
    case TaskResult::Done:
        delete task;
        break;

    case TaskResult::Pending:
        break;

    case TaskResult::Again:
        post(task);
        break;
    }
}
```

规则：

```cpp
Done    -> 自动释放任务
Pending -> 不释放，等待后续重新 post
Again   -> 重新放回当前 Executor
```

---

# 9. 批量分片处理需求

当一批数据需要处理时，先按 shard 分组：

```cpp
template <typename Op>
struct ShardedOps {
    std::vector<std::vector<Op>> shards;
};
```

分片逻辑：

```cpp
for (auto& op : ops) {
    int shard = get_shard(op.key);
    sharded_ops[shard].push_back(std::move(op));
}
```

然后并行调度到多个 shard executor。

---

# 10. 普通无序批处理接口

普通批量任务可以只调度非空 shard：

```cpp
Runtime::parallel_shards(
    sharded_ops,
    ParallelMode::NonEmptyOnly,
    on_all_done
);
```

适合：

```cpp
批量给玩家发奖励
批量刷新若干对象
批量处理独立请求
```

语义：

```cpp
空 shard 不调度
只等待非空 shard 完成
```

---

# 11. 有序 batch 处理接口

对于有顺序要求的增改删 batch，必须调度所有 shard。

```cpp
Runtime::parallel_shards(
    sharded_ops,
    ParallelMode::AllShards,
    batch_id,
    on_all_done
);
```

适合：

```cpp
增量同步
主从复制
全局变更日志
有 batch_id 的增改删流
需要保证所有 shard 按顺序 apply
```

语义：

```cpp
每个 batch 都投递到所有 shard
空 shard 也执行 no-op
每个 shard 都推进 last_applied_batch_id
主任务等待所有 shard 完成
```

---

# 12. ParallelMode

```cpp
enum class ParallelMode {
    NonEmptyOnly, // 只调度有数据的 shard
    AllShards     // 所有 shard 都调度，包括空 shard
};
```

规则：

```cpp
普通无序数据：
    使用 NonEmptyOnly

有序增改删 batch：
    使用 AllShards
```

---

# 13. 有序 batch 的核心规则

对于有序增改删数据：

```cpp
Batch 1 -> shard 0,1,2,3,4,5 有数据
Batch 2 -> shard 0,1,3,4,5 有数据，shard 2 没数据
```

Batch 2 仍然必须调度到 shard 2。

因为 shard 2 虽然没有数据，但也必须确认：

```cpp
shard 2 已经推进到 Batch 2
```

每个 shard 维护：

```cpp
uint64_t last_applied_batch_id;
```

执行时检查：

```cpp
assert(batch_id == last_applied_batch_id + 1);
```

完成后：

```cpp
last_applied_batch_id = batch_id;
```

即使当前 shard 没有数据，也要更新版本。

---

# 14. 有序 CRUD 操作接口

CRUD 操作可以抽象为：

```cpp
enum class OpType {
    Add,
    Update,
    Delete
};

template <typename Key, typename Value>
struct CrudOp {
    OpType type;
    Key key;
    Value value;
    uint64_t batch_id;
};
```

有序 batch：

```cpp
struct ChangeBatch {
    uint64_t batch_id;
    std::vector<CrudOp<Key, Value>> ops;
};
```

处理入口：

```cpp
start_task<ApplyChangeBatchTask>(std::move(batch));
```

任务内部：

```cpp
class ApplyChangeBatchTask : public Task {
public:
    void do_it(ChangeBatch batch) {
        batch_ = std::move(batch);
        state_ = State::Split;

        Runtime::post(ExecutorId{ExecutorType::Logic, 0}, this);
    }

private:
    enum class State {
        Split,
        ApplyShards,
        Finish
    };

    TaskResult run() override {
        switch (state_) {
        case State::Split:
            split_by_shard();
            state_ = State::ApplyShards;

            Runtime::parallel_shards(
                sharded_ops_,
                ParallelMode::AllShards,
                batch_.batch_id,
                this
            );

            return TaskResult::Pending;

        case State::ApplyShards:
            state_ = State::Finish;
            return TaskResult::Again;

        case State::Finish:
            return TaskResult::Done;
        }

        return TaskResult::Done;
    }

private:
    ChangeBatch batch_;
    ShardedOps<CrudOp<Key, Value>> sharded_ops_;
    State state_ = State::Split;
};
```

---

# 15. 并行 shard 处理完成后唤醒主任务

框架内部需要一个 fan-out/fan-in 机制：

```cpp
struct ParallelGroup {
    std::atomic<int> pending;
    Task* owner;
    ExecutorId resume_executor;
};
```

逻辑：

```cpp
主任务分发 N 个 shard 子任务
主任务返回 Pending
每个 shard 完成后 pending--
最后一个 shard 把主任务 post 回 resume_executor
主任务进入下一个状态
```

---

# 16. 最终 API 需求汇总

框架对外核心 API 尽量保持这些：

```cpp
start_task<T>(args...);

Runtime::post(executor_id, task);

Runtime::current_executor();

Runtime::parallel_shards(
    sharded_ops,
    ParallelMode mode,
    Task* owner
);

Runtime::parallel_shards(
    sharded_ops,
    ParallelMode mode,
    uint64_t batch_id,
    Task* owner
);
```

业务任务只需要实现：

```cpp
void do_it(args...);
TaskResult run() override;
```

---

# 17. 一句话总结

框架可以定义为：

> 一个基于固定 Executor/EventLoop 的 C++ 异步任务框架。业务通过 `start_task<T>()` 启动任务，任务在 `do_it()` 中接收参数并首次调度，在 `run()` 中推进状态机。任务完成后由框架自动释放。数据按 shard 绑定到固定线程，普通批处理可只调度非空 shard；有序增改删 batch 必须调度所有 shard，包括空 shard，以保证所有线程按 batch_id 顺序推进。