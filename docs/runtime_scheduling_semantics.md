# Runtime 调度语义

日期：2026-06-03

本文档说明 AsyncFlow runtime 的任务投递语义，重点是 runtime 内部线程、自身目标线程、外部线程三类生产者在 `Auto` / `Fast` / `Ordered` 模式下分别走哪条队列。

## 队列拓扑

runtime 每个固定线程都有三类入口：

- executor 本地 bounded queue：只服务当前 executor 自身生产的 same-thread work，不需要跨线程同步。
- source -> target bounded SPSC queue：runtime 线程之间一对一投递，每个 source 到每个 target 一条队列。
- target bounded MPSC ingress queue：多个生产者共享一个目标线程入队顺序，外部线程和显式 ordered runtime 投递都会使用它。

这个拓扑的目标是把默认热路径保持在最低同步成本：

- runtime 线程投递给自己时默认走本地队列。
- runtime 线程投递给其他 runtime 线程时默认走 SPSC。
- 非 runtime 线程只能通过目标 MPSC 进入 runtime。

## 调度模式

`ScheduleMode::Auto` 是默认模式。它优先选择最低开销的 runtime 路由：

- runtime 线程 -> 自身目标线程：executor 本地 bounded queue。
- runtime 线程 -> 其他 runtime 线程：source -> target SPSC queue。
- 外部线程 -> runtime 目标线程：target MPSC ingress queue。

`ScheduleMode::Fast` 表示调用者明确要求 runtime 线程低开销路径：

- runtime 线程 -> 自身目标线程：executor 本地 bounded queue。
- runtime 线程 -> 其他 runtime 线程：source -> target SPSC queue。
- 外部线程调用会失败，不会隐式降级到 MPSC。

`ScheduleMode::Ordered` 表示调用者明确要求目标线程上的统一入队顺序：

- runtime 线程 -> 自身目标线程：target MPSC ingress queue。
- runtime 线程 -> 其他 runtime 线程：target MPSC ingress queue。
- 外部线程 -> runtime 目标线程：target MPSC ingress queue。

因此，runtime 内部线程投递到自身目标时也可以选择语义：

- 使用 `schedule_fast(thread)` / `pending_fast(thread)`：保持本地队列快速路径。
- 使用 `schedule_ordered(thread)` / `pending_ordered(thread)`：强制走目标 MPSC，与其他生产者共享同一个目标入队顺序。

## API 选择建议

普通状态机切线程优先使用默认 API：

```cpp
return pending_on(TargetThread);
```

如果调用点已经确认在 runtime 线程内，并且只需要低开销投递，不需要和外部生产者共享目标顺序，可以使用 fast API：

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

不要把 `Fast` 当作“更快版本的 Ordered”。`Fast` 的语义是 runtime-thread-only 低同步成本路径；它不提供多个生产者汇聚到同一个目标线程时的全局入队顺序。

## 队列满载语义

队列容量由 runtime traits 配置：

- `spsc_queue_capacity`：runtime 内部 local/SPSC 路径容量。
- `external_queue_capacity`：目标 MPSC ingress 容量。

满队列策略由 `QueueFullPolicy` 控制：

- `Reject`：入队失败时立即返回 `false`，不会阻塞生产者。
- `Yield`：生产者等待空位，等待过程会执行 CPU relax / yield backoff。

满队列策略需要按生产者类型分别配置，旧的单一 `queue_full_policy` 不再作为 traits 入口：

- `runtime_queue_full_policy`：runtime 线程生产者策略。
- `external_queue_full_policy`：外部线程生产者策略。

这允许 runtime 内部高优先级路径使用 `Yield`，外部入口仍保持 `Reject`，避免外部生产者在满载时拖住业务线程。

## 正确性覆盖

相关测试：

- `RuntimeBackpressureTests.RejectPolicyReturnsFalseAndDeletesRejectedTask`
- `RuntimeBackpressureTests.YieldPolicyAllowsManyExternalProducers`
- `RuntimeBackpressureTests.YieldPolicyHandlesSameThreadFanoutWithBoundedLocalQueue`
- `RuntimeBackpressureTests.SplitQueuePoliciesRejectFullExternalQueueWithoutBlockingProducer`
- `RuntimeBackpressureTests.SplitQueuePoliciesKeepRuntimeThreadFanoutOnYieldPolicy`
- `RuntimeBackpressureTests.OrderedSelfPostUsesMpscWhenLocalQueueIsFull`

其中 `OrderedSelfPostUsesMpscWhenLocalQueueIsFull` 专门验证 self-post 场景：

- 先让 runtime 线程把自身 local queue 填满。
- 再用默认 `Auto` 投递自身目标，预期因为 local queue 满而失败。
- 再用 `ScheduleMode::Ordered` 投递自身目标，预期成功进入目标 MPSC。

这个测试直接证明 runtime 内部线程投递自身目标时，可以显式选择走 MPSC 顺序路径。
