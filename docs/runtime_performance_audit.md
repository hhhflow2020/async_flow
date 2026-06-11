# Runtime 性能审计

## 调度

- 每个 executor 使用一个 intrusive unbounded MPSC inbox。
- runtime 线程、外部线程和 same-thread self-post 共享目标 executor admission order。
- 调度不再暴露 `Fast` / `Ordered` 模式；统一 inbox 是唯一 admission 语义。
- executor 阻塞前先 drain task，避免事件线程饥饿。

## Cache 与 false sharing

- executor 独立对象按 cache line 对齐。
- 热原子状态拆分，避免多个线程频繁写同一 cache line。
- task inbox 的生产者热路径是单次 atomic exchange 和前驱 next 写入。
- inbox producer/consumer 游标按 cache line 拆分，降低 false sharing。
- 日志 record pool 和 runtime task pool 都应按线程局部复用。
- 对象池 slot 已按 cache line 对齐，block/pool 热原子字段独立对齐；日志 record pool 的 slab free-list 和扩容标志也与热路径数据分离。

## 分支与系统调用

- 调度热路径不需要按模式分支选择不同队列。
- IO readiness 只在 interest 变化时更新 poller。
- eventfd/user event 唤醒合并，避免无意义 syscall。
- 网络读写 drain 到 would-block 或预算耗尽。

## 仍可提升

- 为 TCP reactor 增加连接读写预算和批量 ready 队列。
- 为 IO buffer 增加按线程固定大小池。
- 为 ordered logging 的单 MPSC 队列补充高并发 perf regression 测试。
- 持续记录 `async_log_record_pool` 单线程、批量和跨线程 release benchmark 结果，避免日志池优化退化。
- 对 epoll/kqueue poll batch 大小做 benchmark。
