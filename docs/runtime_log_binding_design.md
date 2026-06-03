# Runtime 绑定日志设计

异步日志消费者默认绑定到 runtime 线程，不创建独立消费线程。

## 线程选择

优先级：

1. 显式配置的 consumer thread。
2. `ThreadKind::Log`。
3. `ThreadKind::Io` / `Epoll` / `Kqueue`。
4. thread index 0。

## 数据路径

- runtime 线程生产日志：进入该线程对应的 SPSC lane。
- 外部线程生产日志：进入 bounded MPSC ingress。
- consumer task 在绑定线程上批量 drain。
- record pool 批量回收，减少逐条分配。
- 文件、UDP、TCP 后端由 consumer task 调用。

## 顺序策略

默认 ordered 策略使用单 MPSC 队列，尽量保持全局到达顺序。高吞吐 relaxed 策略可使用分片队列，降低多生产者竞争，但只保证每个生产者内部 FIFO。

## 后端

- 文件后端：批量 `writev` 或等价聚合写。
- UDP 后端：批量构造 datagram 后在绑定 IO 线程发送。
- TCP 后端：维护连接状态和写队列，在绑定 IO 线程 flush。

## 正确性

- 日志等级过滤在前端完成，不匹配等级不格式化用户消息。
- runtime task id 插入用户日志字段开头，不改变整行统一前缀。
- consumer 停止前需要 drain 已接受 record。
