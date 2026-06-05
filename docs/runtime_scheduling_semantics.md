# Runtime 调度语义

日期：2026-06-03

本文档说明 AsyncFlow runtime 的任务投递语义，重点是 runtime 内部线程、自身目标线程、外部线程三类生产者如何进入目标 executor。

## 队列拓扑

runtime 每个固定线程拥有一个入口：

- target intrusive unbounded MPSC inbox：所有生产者都通过目标 executor 的 inbox 入队。

这个拓扑的目标是把调度正确性放在一个清晰的入队点上：

- 没有 local queue 和 source -> target SPSC 的双路径顺序问题。
- same-thread、cross-thread、external producer 共享同一个目标 admission order。
- 生产者使用 intrusive node，不为每次 task 投递分配队列节点。

## 统一调度语义

所有 task 调度都进入目标 executor 的 intrusive MPSC inbox：

- runtime 线程 -> 自身目标线程：target intrusive MPSC inbox。
- runtime 线程 -> 其他 runtime 线程：target intrusive MPSC inbox。
- 外部线程 -> runtime 目标线程：target intrusive MPSC inbox。

因此，runtime 内部线程投递到自身目标时不再有隐藏 local queue。当前 runtime 不再暴露 `Auto` / `Fast` / `Ordered` 调度模式：统一 inbox 本身就是顺序语义，外部线程是否允许调用由具体 API 的使用契约和对象 owner 线程约束表达。

## API 选择建议

普通状态机切线程优先使用默认 API：

```cpp
return pending_to(TargetThread);
```

首次启动任务时同理：

```cpp
return schedule_to(TargetThread);
```

延迟调度使用同一套目标 admission 入口，但不会直接进入 ready 队列：

```cpp
return schedule_after(TargetThread, 20ms);
return pending_after(TargetThread, 20ms);
```

延迟 task 先以 `TimerArming` 状态进入目标 executor inbox，目标线程再挂入本地 timer heap。timer 到期后由目标 executor 转为 `Queued` 并执行。这个路径保持 timer 结构单线程访问，不引入锁，也不会恢复旧的 local queue/SPSC 双路径。

旧 `ScheduleMode` / `schedule_mode` 兼容枚举已经移除。新代码只使用 `schedule_to(...)` / `pending_to(...)`，让“切到目标线程”的语义保持直接。

## 队列满载语义

task inbox 是 intrusive unbounded MPSC，不再用队列容量拒绝任务投递。只要 task 对象已经成功创建，目标 inbox 不会因为容量满而返回失败。

历史 task 队列容量和满载策略 traits 已移除。runtime task 调度不再暴露 `external_queue_capacity`、`runtime_queue_full_policy`、`external_queue_full_policy` 或 `queue_full_spin_count`。日志、网络 batch 等确实有界的队列继续使用各自子系统的配置。

## 正确性覆盖

相关测试：

- `RuntimeBackpressureTests.UnboundedInboxAcceptsTasksBehindBlockingOwner`
- `RuntimeBackpressureTests.UnboundedInboxAllowsManyExternalProducers`
- `RuntimeBackpressureTests.RuntimeThreadFanoutUsesUnifiedInbox`
- `RuntimeBackpressureTests.SameThreadRuntimeProducerUsesUnifiedInbox`
- `RuntimeFixture.ScheduleToAliasRunsOnRequestedThread`
- `RuntimeFixture.PendingToAliasResumesOnRequestedThread`
- `RuntimeFixture.DelayedStartRunsOnRequestedThreadAfterDelay`
- `RuntimeFixture.PendingAfterResumesOnRequestedThreadAfterDelay`
- `RuntimeShutdownTests.StopImmediatelyCancelsAndDestroysDelayedTasks`

其中 `SameThreadRuntimeProducerUsesUnifiedInbox` 专门验证 self-post 场景：

- runtime 线程投递自身目标时进入统一 inbox。
- owner 正在运行阻塞任务时，后续任务仍能进入同一个 unbounded inbox，证明 task 调度不再受旧 bounded 队列容量限制。

这个测试直接证明 runtime 内部线程投递自身目标时，不再存在 local queue 和 MPSC 的双路径顺序差异。
