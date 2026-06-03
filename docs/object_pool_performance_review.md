# 对象池性能审计

## 当前实现

- task 对象池按具体 task 类型分离。
- 每个 block 使用固定 slot，slot index 和 generation 组合避免 ABA 风险。
- 本线程释放优先进入 local cache。
- 跨线程释放进入 remote batch，批量归还。
- pool 支持预留 block 和 slot，降低启动后扩容抖动。

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

## 建议

- 对高频 task 设置合理的 `task_pool_chunk_size`。
- 让 `task_pool_remote_release_batch_size` 小于等于 local cache capacity。
- 压测时同时观察吞吐、p99 延迟和 resident memory。
- 对网络连接、日志 record、buffer 等对象维持按线程局部池化。
