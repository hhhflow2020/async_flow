# Runtime 性能审计

## 调度

- runtime 线程之间使用 source -> target SPSC 队列。
- 外部入口使用 bounded MPSC 队列。
- 自身投递自身可走 local queue，顺序语义可强制走 MPSC。
- executor 阻塞前先 drain task，避免事件线程饥饿。

## Cache 与 false sharing

- executor 独立对象按 cache line 对齐。
- 热原子状态拆分，避免多个线程频繁写同一 cache line。
- SPSC 队列按线程对拆分，减少多生产者共享写指针。
- 日志 record pool 和 runtime task pool 都应按线程局部复用。

## 分支与系统调用

- 调度模式在 API 层显式表达，减少热路径猜测。
- IO readiness 只在 interest 变化时更新 poller。
- eventfd/user event 唤醒合并，避免无意义 syscall。
- 网络读写 drain 到 would-block 或预算耗尽。

## 仍可提升

- 为 TCP reactor 增加连接读写预算和批量 ready 队列。
- 为 IO buffer 增加按线程固定大小池。
- 为 ordered logging 的单 MPSC 队列补充高并发 perf regression 测试。
- 对 epoll/kqueue poll batch 大小做 benchmark。
