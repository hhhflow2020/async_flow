# 对象池性能审计

## 当前实现

- task 对象池按具体 task 类型分离。
- 每个 block 使用固定 slot，slot index 和 generation 组合避免 ABA 风险。
- 本线程释放优先进入 local cache。
- 跨线程释放进入 remote batch，批量归还。
- pool 支持预留 block 和 slot，降低启动后扩容抖动。
- task pool holder 的预热状态使用 cache-line 原子隔离，避免任务创建热路径与 pool 热字段 false sharing。
- slot 按 cache line 对齐，避免不同对象共享同一 cache line 造成 false sharing。
- block free-list 使用带版本号的 64-bit CAS，不使用 mutex。
- TLS local cache 使用 pool token 区分同地址复用；pool 析构时先失效 lifetime token，本线程 cache 直接丢弃，其他线程在线程退出/flush 时发现 pool 已失效后丢弃，不会把远端释放缓存回写到已析构 pool。

## 日志 record pool

- `async_log_record_pool` 使用固定数组 TLS local cache，命中时 acquire/release 不进入全局锁，也不需要在线程首次绑定 pool 时为 cache 做动态分配。
- TLS local cache 同样使用 pool token 和 weak lifetime，pool 地址复用或 pool 已析构时只丢弃 stale cache，不触碰已释放 slab。
- 每个 slab 有独立的带版本号 free-list；批量 release 会按 slab 分组，只对同一 slab 做一次链表拼接和 CAS。
- local cache refill 会跨已有 slab 尽量填满缓存，避免某个 slab 只剩少量 slot 时频繁重复进入全局 free-list。
- 扩容通过 cache-line 隔离的 `atomic<bool>` CAS 门闩串行化。抢不到扩容权的生产者只读观察门闩并优先复用已发布 slab，避免 `test_and_set` 写自旋造成扩容冷路径上的 cache line 来回失效；正常日志热路径不加锁。
- `log_record` 按 cache line 对齐，默认 1024 字节 inline message，普通日志不会触发 heap 分配。

## IO buffer

- `af::buffer` 不再维护自研 TLS size-class storage，直接使用 Folly `IOBuf` 作为底层存储。
- `with_capacity()` 使用 `IOBuf::createCombined()` 预留连续 headroom/tailroom，避免 `std::vector<std::byte>` 的整块 value-initialize。
- copy/slice 路径依赖 `cloneOne()` 的共享底层数据；mutable 路径进入 `writableData()`/`writableTail()` 前先 `unshareOne()`，避免共享视图被写穿。
- `capacity()`、`headroom()` 和 `tailroom()` 继续保留逻辑容量语义，后续零拷贝链式输出应优先复用 Folly IOBuf 的块链能力，而不是恢复框架内 buffer pool。

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
- 并发突发耗尽所有 slab 时，只有一个生产者真正分配新 slab，其他生产者在只读等待后复用已发布空闲 slot；该路径仍是冷路径，不应该替代容量预热。

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

## 补充远端复核

远端容器输出时间：`2026-06-12T04:23Z`。命令参数：
`--benchmark_min_time=0.05s --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`。

结果摘要：

- `BM_ObjectPoolCreateDestroy/1024_mean`：约 `418.2M items/s`，CV `0.33%`。
- `BM_ObjectPoolBatchCreateDestroy/1024_mean`：约 `141.3M items/s`，CV `0.84%`。
- `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/1024_mean`：约 `52.4M items/s`，CV `4.08%`。
- `BM_AsyncLogRecordPoolAcquireRelease/256_mean`：约 `75.7M items/s`，CV `2.49%`。
- `BM_AsyncLogRecordPoolAcquireRelease/1024_mean`：约 `65.5M items/s`，CV `0.61%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/64/256_mean`：约 `116.6M items/s`，CV `0.62%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/1024/1024_mean`：约 `56.5M items/s`，CV `0.93%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/64/256/real_time_mean`：约 `20.3M items/s`，CV `0.57%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/1024/1024/real_time_mean`：约 `21.1M items/s`，CV `5.48%`。

判断：

- 对象池本线程 create/destroy 与批量路径相对前次复核没有退化，批量 create/destroy 略高。
- 对象池跨线程 remote batch 仍稳定在约 `52M items/s`，主要成本仍是跨线程同步和缓存一致性流量。
- 日志 record pool 本线程、批量、跨线程 release 三类关键路径均能稳定出数；`1024/1024` 跨线程项 CV 偏高，后续若做 release batch 调参，应继续用该项观察尾部抖动。

## 2026-06-12 TLS 生命周期修复复核

本轮补充 `ObjectPoolRemoteReleaseCacheDiscardsAfterPoolDestruction`，覆盖跨线程 remote release 缓存在 pool 析构后仍滞留在线程 TLS 中的边界。修复后 remote-release TLS cache 在 pool lifetime 失效时丢弃本地缓存，避免线程退出时回写已析构 pool。

远端容器输出时间：`2026-06-12T08:18Z`。命令参数：
`--benchmark_filter='BM_ObjectPoolCreateDestroy/16384|BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384|BM_AsyncLogRecordPoolAcquireRelease/256' --benchmark_min_time=0.2s`。

结果摘要：

- `BM_AsyncLogRecordPoolAcquireRelease/256`：约 `77.9M items/s`。
- `BM_ObjectPoolCreateDestroy/16384`：约 `250.6M items/s`。
- `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384`：约 `63.1M items/s`。

判断：

- 对象池新增 token 校验后，本线程 create/destroy 和跨线程 batch destroy 仍能稳定出数。
- weak lifetime 只在 cache reset/flush/线程退出等冷路径触碰；热路径保持 local cache + free-list CAS，不引入锁。

## 2026-06-12 日志池扩容门闩复核

本轮补充 `SharedRecordPoolExpandsUnderConcurrentAcquirePressure` 和 `LogRecordPoolExpansionAvoidsAtomicFlagWriteSpin`，固定日志 record pool 在极小初始 slab 下的并发扩容正确性，并禁止扩容冷路径恢复为 `atomic_flag::test_and_set` 写自旋。

远端容器输出时间：`2026-06-12T08:53Z`。命令参数：
`--benchmark_filter=AsyncLogRecordPool --benchmark_min_time=0.05s --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`。

结果摘要：

- `BM_AsyncLogRecordPoolAcquireRelease/256_mean`：约 `79.3M items/s`，CV `0.17%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/64/256_mean`：约 `118.0M items/s`，CV `0.23%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/1024/1024_mean`：约 `54.4M items/s`，CV `0.02%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/64/256/real_time_mean`：约 `20.5M items/s`，CV `0.71%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/1024/1024/real_time_mean`：约 `22.3M items/s`，CV `1.32%`。

判断：

- 扩容门闩从 `atomic_flag` 改为 `atomic<bool>` CAS 后，热路径 benchmark 与前次复核同量级，没有观察到本线程、批量或跨线程 release 路径退化。
- 扩容等待者只读观察门闩并优先复用已发布 slab，可以减少突发扩容时的 producer cache line 写争用；该收益主要体现在冷路径争用行为，热路径 benchmark 用于确认没有引入额外成本。

## 2026-06-18 Folly IOBuf 迁移后复核

本轮将 `af::buffer` 从框架自研 TLS IO buffer pool 迁移为 Folly `IOBuf` 底层存储。远端 gcc Release clean build 和全量 `ctest` 均通过：`312` 个测试通过，`4` 个平台/场景测试按逻辑跳过。

远端容器输出时间：`2026-06-18T08:52Z`。命令参数：
`--benchmark_filter='ObjectPool|AsyncLogRecordPool' --benchmark_min_time=0.05s --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`。

结果摘要：

- `BM_AsyncLogRecordPoolAcquireRelease/256_mean`：约 `77.4M items/s`，CV `0.04%`。
- `BM_AsyncLogRecordPoolBatchAcquireRelease/64/256_mean`：约 `118.5M items/s`，CV `0.05%`。
- `BM_AsyncLogRecordPoolCrossThreadReleaseBatch/64/256/real_time_mean`：约 `21.5M items/s`，CV `1.14%`。
- `BM_ObjectPoolCreateDestroy/1024_mean`：约 `333.9M items/s`，CV `1.02%`。
- `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/1024_mean`：约 `54.3M items/s`，CV `1.35%`。
- `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384_mean`：约 `68.9M items/s`，CV `0.30%`。

判断：

- buffer 存储迁移没有改变 task 对象池和日志 record pool 的热路径结构；对象池仍是 local cache + tagged free-list，日志池仍是 TLS cache + slab free-list。
- 日志 record pool 本线程和批量路径与前次复核同量级；跨线程 release 仍主要受原子同步和 cache coherency 流量影响。
- 对象池本线程 create/destroy 与 remote batch 跨线程回收均稳定出数，没有观察到 Folly IOBuf 依赖引入后导致 pool 热路径退化。

## 建议

- 对高频 task 设置合理的 `task_pool_chunk_size`。
- 让 `task_pool_remote_release_batch_size` 小于等于 local cache capacity。
- 压测时同时观察吞吐、p99 延迟和 resident memory。
- 对网络连接、日志 record 等框架自管对象维持按线程局部池化；buffer 复用交给 Folly IOBuf，不再在框架内维护第二套 IO buffer pool。
- 日志压测需要分别覆盖 ordered logger、runtime lane、外部 producer、record pool acquire/release、跨线程批量 release 五类路径。
