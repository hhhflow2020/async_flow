# 对象池性能审计

## 当前实现

- task 对象池按具体 task 类型分离。
- 每个 block 使用固定 slot，slot index 和 generation 组合避免 ABA 风险。
- 本线程释放优先进入 local cache。
- 跨线程释放进入 remote batch，批量归还。
- pool 支持预留 block 和 slot，降低启动后扩容抖动。
- slot 按 cache line 对齐，避免不同对象共享同一 cache line 造成 false sharing。
- block free-list 使用带版本号的 64-bit CAS，不使用 mutex。

## 日志 record pool

- `AsyncLogRecordPool` 使用 TLS local cache，命中时 acquire/release 不进入全局锁。
- 每个 slab 有独立的带版本号 free-list；批量 release 会按 slab 分组，只对同一 slab 做一次链表拼接和 CAS。
- 扩容通过 `atomic_flag` 串行化，只在 slab 耗尽时进入冷路径；正常日志热路径不加锁。
- `LogRecord` 按 cache line 对齐，默认 1024 字节 inline message，普通日志不会触发 heap 分配。

## 性能判断

优点：

- task 创建不走通用 heap 热路径。
- 同类型对象复用有利于 cache locality。
- remote release batch 降低跨线程原子操作频率。
- local cache set 可配置，能控制内存占用和复用命中率。

风险：

- block 扩容仍可能在突发创建时发生。
- 过大的 local cache 会提高驻留内存。
- 过小的 remote batch 会增加跨线程回收开销。
- `AsyncLogRecordPool` 的 TLS cache 第一次绑定 pool 时会扩容 `std::vector`，高频日志线程建议在启动阶段预热。
- 日志 record pool 的 slab 列表扩容仍是全局冷路径，高峰前应通过 `record_pool.slab_object_count` 配置足够初始容量。

## 复核覆盖

- 对象池已有 benchmark 覆盖 create/destroy、批量 create/destroy、冷启动 burst、跨线程 destroy、fan-in、round-robin、多池交替和 tuned remote batch/cache capacity。
- 本轮新增日志 record pool benchmark：
  - `BM_AsyncLogRecordPoolAcquireRelease`
  - `BM_AsyncLogRecordPoolBatchAcquireRelease`
  - `BM_AsyncLogRecordPoolCrossThreadReleaseBatch`
- 建议性能回归时至少运行：

```bash
./asyncflow_runtime_benchmarks --benchmark_filter='ObjectPool|AsyncLogRecordPool' --benchmark_min_time=0.1s
```

## 建议

- 对高频 task 设置合理的 `task_pool_chunk_size`。
- 让 `task_pool_remote_release_batch_size` 小于等于 local cache capacity。
- 压测时同时观察吞吐、p99 延迟和 resident memory。
- 对网络连接、日志 record、buffer 等对象维持按线程局部池化。
- 日志压测需要分别覆盖 ordered logger、runtime lane、外部 producer、record pool acquire/release、跨线程批量 release 五类路径。
