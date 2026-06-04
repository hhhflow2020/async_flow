# Runtime 调度语义

日期：2026-06-03

本文档说明 AsyncFlow runtime 的任务投递语义，重点是 runtime 内部线程、自身目标线程、外部线程三类生产者在 `Auto` / `Fast` / `Ordered` 模式下如何进入目标 executor。

## 队列拓扑

runtime 每个固定线程拥有一个入口：

- target intrusive unbounded MPSC inbox：所有生产者都通过目标 executor 的 inbox 入队。

这个拓扑的目标是把调度正确性放在一个清晰的入队点上：

- 没有 local queue 和 source -> target SPSC 的双路径顺序问题。
- same-thread、cross-thread、external producer 共享同一个目标 admission order。
- 生产者使用 intrusive node，不为每次 task 投递分配队列节点。

## 调度模式

`ScheduleMode::Auto` 是默认模式：

- runtime 线程 -> 自身目标线程：target intrusive MPSC inbox。
- runtime 线程 -> 其他 runtime 线程：target intrusive MPSC inbox。
- 外部线程 -> runtime 目标线程：target intrusive MPSC inbox。

`ScheduleMode::Fast` 表示调用者明确要求 runtime 线程生产者：

- runtime 线程 -> 目标线程：target intrusive MPSC inbox。
- 外部线程调用会失败。

`ScheduleMode::Ordered` 表示调用者明确强调目标线程上的统一入队顺序：

- runtime 线程 -> 目标线程：target intrusive MPSC inbox。
- 外部线程 -> runtime 目标线程：target intrusive MPSC inbox。

因此，runtime 内部线程投递到自身目标时不再有隐藏 local queue；需要表达“只允许 runtime 线程调用”时使用 `Fast`，需要在调用点强调统一顺序语义时使用 `Ordered`。

## API 选择建议

普通状态机切线程优先使用默认 API：

```cpp
return pending_on(TargetThread);
```

如果调用点已经确认在 runtime 线程内，并且外部线程调用应被拒绝，可以使用 fast API：

```cpp
return pending_fast(TargetThread);
```

如果多个生产者必须在同一个目标线程上保持统一 admission order，使用 ordered API：

```cpp
return pending_ordered(TargetThread);
```

首次启动任务时同理：

```cpp
return schedule(TargetThread);          // 默认 Auto
return schedule_fast(TargetThread);     // runtime-only 快速路径
return schedule_ordered(TargetThread);  // 强制目标 MPSC 顺序路径
```

不要把 `Fast` 当作“更快版本的 Ordered”。当前实现下二者都进入目标 inbox；`Fast` 的语义是 runtime-thread-only，`Ordered` 的语义是调用点显式要求目标 admission order。

## 队列满载语义

task inbox 是 intrusive unbounded MPSC，不再用队列容量拒绝任务投递。只要 task 对象已经成功创建，目标 inbox 不会因为容量满而返回失败。

历史 task 队列容量和满载策略 traits 已移除。runtime task 调度不再暴露 `external_queue_capacity`、`runtime_queue_full_policy`、`external_queue_full_policy` 或 `queue_full_spin_count`。日志、网络 batch 等确实有界的队列继续使用各自子系统的配置。

## 正确性覆盖

相关测试：

- `RuntimeBackpressureTests.UnboundedInboxAcceptsTasksBehindBlockingOwner`
- `RuntimeBackpressureTests.UnboundedInboxAllowsManyExternalProducers`
- `RuntimeBackpressureTests.RuntimeThreadFanoutUsesUnifiedInbox`
- `RuntimeBackpressureTests.SameThreadAutoAndOrderedUseUnifiedInbox`

其中 `SameThreadAutoAndOrderedUseUnifiedInbox` 专门验证 self-post 场景：

- runtime 线程投递自身目标时，`Auto` 与 `Ordered` 都进入统一 inbox。
- owner 正在运行阻塞任务时，后续任务仍能进入同一个 unbounded inbox，证明 task 调度不再受旧 bounded 队列容量限制。

这个测试直接证明 runtime 内部线程投递自身目标时，不再存在 local queue 和 MPSC 的双路径顺序差异。
