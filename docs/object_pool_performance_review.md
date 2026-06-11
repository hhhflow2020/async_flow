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

- `async_log_record_pool` 使用固定数组 TLS local cache，命中时 acquire/release 不进入全局锁，也不需要在线程首次绑定 pool 时为 cache 做动态分配。
- 每个 slab 有独立的带版本号 free-list；批量 release 会按 slab 分组，只对同一 slab 做一次链表拼接和 CAS。
- local cache refill 会跨已有 slab 尽量填满缓存，避免某个 slab 只剩少量 slot 时频繁重复进入全局 free-list。
- 扩容通过 `atomic_flag` 串行化，只在 slab 耗尽时进入冷路径；正常日志热路径不加锁。
- `log_record` 按 cache line 对齐，默认 1024 字节 inline message，普通日志不会触发 heap 分配。

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
- `async_log_record_pool` 的 TLS cache 容量上限为 4096 条 record 指针；这是每线程缓存上限，不是 pool 总容量上限。pool 仍会按 slab 持续扩展到 OOM。
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

## 2026-06-11 远端复核

环境：

- 机器：`root@192.168.31.192` 的 `/data/async_flow-next-runtime-architecture`
- 容器：`ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`
- 构建：`build-gcc/build/Release`
- 命令参数：`--benchmark_min_time=0.1s --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`

结果摘要：

- `BM_ObjectPoolCreateDestroy/1024_mean`：约 `414.9M items/s`，CV `1.92%`。
- `BM_ObjectPoolBatchCreateDestroy/1024_mean`：约 `132.9M items/s`，CV `0.65%`。
- `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/1024_mean`：约 `52.3M items/s`，CV `5.14%`。
- `BM_AsyncLogRecordPoolAcquireRelease/256_mean`：约 `78.3M items/s`，CV `0.11%`。
- `BM_AsyncLogRecordPoolAcquireRelease/1024_mean`：约 `63.9M items/s`，CV `0.16%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/64/256_mean`：约 `121.6M items/s`，CV `0.35%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/1024/1024_mean`：约 `55.9M items/s`，CV `0.31%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/64/256/real_time_mean`：约 `20.9M items/s`，CV `0.21%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/1024/1024/real_time_mean`：约 `22.4M items/s`，CV `0.38%`。

判断：

- 本地缓存命中和批量路径稳定，CV 多数低于 `1%`，说明 cache-line 对齐、TLS local cache 和批量 slab release 的热路径没有明显抖动。
- 跨线程对象池 remote batch 路径吞吐低于本线程路径，符合跨线程原子写入和缓存一致性开销预期；后续如继续优化，应优先比较 remote batch size、direct release set size 和线程 fan-in 模式，而不是引入锁。
- 日志 record pool 的跨线程批量释放在 `64/256` 与 `1024/1024` 配置下吞吐接近，说明按 slab 分组批量归还能降低大批量释放时的 CAS 次数。
- 本轮补充 `RecordPoolLocalCacheRefillFillsAcrossExistingSlabs` 回归测试，固定 local cache refill 跨已有 slab 补满缓存的行为。远端单次 benchmark 冒烟中，`BM_AsyncLogRecordPoolAcquireRelease/256` 约 `74.2M items/s`，`BM_AsyncLogRecordPoolBatchAcquireRelease/64/256` 约 `117.9M items/s`，日志池热路径能稳定出数。

## 建议

- 对高频 task 设置合理的 `task_pool_chunk_size`。
- 让 `task_pool_remote_release_batch_size` 小于等于 local cache capacity。
- 压测时同时观察吞吐、p99 延迟和 resident memory。
- 对网络连接、日志 record、buffer 等对象维持按线程局部池化。
- 日志压测需要分别覆盖 ordered logger、runtime lane、外部 producer、record pool acquire/release、跨线程批量 release 五类路径。
