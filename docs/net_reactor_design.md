# 网络 Reactor 设计

当前网络层以 native readiness 为唯一主路径：Linux epoll，macOS/BSD kqueue。IO 线程直接管理 fd、事件、buffer 和连接状态，业务 task 只负责业务计算和跨线程流程。

## 分层

- `EventPoller`：封装 epoll/kqueue 的注册、更新、删除、等待和唤醒。
- `Channel`：保存 fd、interest、ready events、owner 和回调。
- `TcpListener`：监听 fd、accept 循环、连接创建。
- `TcpConnection`：连接 fd、读 buffer、写队列、关闭状态和用户上下文。
- `TcpServer`：绑定线程组、监听地址、连接生命周期回调和读事件回调。
- `UdpSocket` / `UnixSocket`：复用 channel 抽象，按协议差异处理读写。
- `TcpConnectionHandle`：由 IO 线程、slot、generation 组成，业务层可安全保存。

## IO 线程循环

单个 IO executor 的推荐顺序：

1. drain 已经调度到本线程的 task。
2. 处理本地队列和跨线程 SPSC/MPSC ingress。
3. 非阻塞 poll 一次 fd events。
4. 如果没有 task 和事件，进入 epoll/kqueue 阻塞等待。
5. 被 eventfd/user event 唤醒后先 drain task，再处理 fd events。

这样可以避免 IO 线程长期阻塞导致同线程 task 饥饿。

## TCP Server API 方向

示意：

```cpp
af::net::TcpServer<Runtime> server;
server.bind_threads(Runtime::thread_group<IoTag>())
      .listen({"0.0.0.0", 8080})
      .reuse_port(true)
      .on_connection([](auto conn) {})
      .on_read([](auto conn, af::BufferView bytes) {})
      .on_close([](auto conn) {})
      .start();
```

核心只提供字节流。packet id、包长度、protobuf/json 解析、业务 dispatcher 由用户代码决定。示例中的 login server 使用“长度 + 包 id + protobuf content”只是业务层组合方式，不进入核心依赖。

## 写入策略

业务 task 要写连接时，推荐调用 `TcpConnectionHandle::send()` 这类线程安全入口。handle 内部把写请求投递到连接所属 IO 线程，由 IO 线程合并写队列、执行 syscall，并按 writable readiness 继续 flush。业务 task 不应直接跨线程操作 fd。

## 连接管理

登录完成后，业务层可维护 `user_id -> TcpConnectionHandle`。handle 带 generation，连接关闭或 slot 复用后，旧 handle 的发送会失败，不会误发到新连接。

## 性能原则

- listener 和 connection fd 尽量只在注册、interest 变化、关闭时修改 poller。
- 读事件 drain 到 `EAGAIN` 或预算耗尽，减少事件风暴。
- 写队列按连接归属 IO 线程管理，避免多线程直接写同一 fd。
- buffer 池按 IO 线程局部分配，减少跨线程释放。
- 多 IO 线程绑定同一端口时使用 `SO_REUSEPORT`，避免一个 listener fd 同时进入多个 poller。
