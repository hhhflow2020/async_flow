# 网络 Reactor 设计

> 本文记录当前网络 reactor 设计和已实现 API。下一代目标架构中，reactor 继续作为 epoll/kqueue/select 的统一抽象，但 `ThreadKind` 收敛为 `io` / `cpu`，跨线程 socket 操作通过 task 显式调度到 owner reactor，不再依赖隐藏 command queue。完整目标方案见 [next_runtime_architecture.md](next_runtime_architecture.md)。

当前网络层以 native readiness 为主路径：Linux 使用 epoll，macOS/BSD 使用 kqueue。IO 线程直接管理 fd、事件、buffer 和连接状态，业务 task 只负责业务计算和跨线程流程。

## 分层

- `EventPoller`：封装 epoll/kqueue 的注册、更新、删除、等待和唤醒。
- `Channel`：保存 fd、interest、ready events、owner 和回调。
- `IpEndpoint`：当前 IPv4/IPv6 endpoint 的轻量表示，并承载 Unix path 作为过渡别名；`TcpEndpoint`、`UdpEndpoint`、`UnixEndpoint` 是语义化别名。
- `TcpListener`：监听 fd、accept 循环、listener id、监听配置和 handler 绑定。
- `TcpConnection`：连接 fd、读 buffer、写队列、关闭状态、所属 listener 和用户上下文。
- `TcpServer`：管理多个 listener、默认 IO 线程组、动态 add/remove listener、启动/停止生命周期。
- `TcpClient`：在绑定 IO 线程上发起非阻塞 connect，连接成功后交给 `TcpConnection` 统一管理。
- `UdpSocket`：绑定本地 IPv4/IPv6 endpoint，可选连接 remote endpoint；同一抽象同时覆盖 UDP server/client。
- `UnixStreamServer` / `UnixStreamClient`：面向 Unix domain stream socket 的 server/client 包装，复用 stream connection 热路径。
- `UnixDatagramSocket`：面向 Unix domain datagram socket 的 server/client 包装，复用 datagram shard 热路径并管理 path 生命周期。
- `TcpConnectionHandle` / `UdpSocketHandle`：线程安全的业务侧句柄，跨线程发送会投递到归属 IO shard。

## IO 线程循环

单个 IO executor 的推荐顺序：

1. drain 已经调度到本线程的 task。
2. 继续 drain 统一 intrusive MPSC task inbox 中新到达的任务。
3. 非阻塞 poll 一次 fd events。
4. 如果没有 task 和事件，进入 epoll/kqueue 阻塞等待。
5. 被 eventfd/user event 唤醒后先 drain task，再处理 fd events。

这样可以避免 IO 线程长期阻塞导致同线程 task 饥饿。

## TCP Server API

`TcpServer` 不以 handler 作为模板参数。handler 绑定在 listener 上，因此同一个 server 可以管理多个监听地址和多种业务入口。

```cpp
af::net::TcpServer<Runtime> server(af::net::TcpServerConfig{
    .command_queue_capacity = 8192,
});

// 在 runtime task 中执行，且后续控制操作固定在同一个 reactor 线程上。
server.bind_threads(Runtime::thread_group<IoTag>());
auto public_listener = server.add_listener<PublicHandler>({
    .name = "public",
    .endpoint = af::net::TcpEndpoint::any(8080),
    .options = {.reuse_port = true},
});
const bool scheduled = public_listener.ok() && server.start();
```

`TcpServerConfig` 是 server/shard 级配置，目前用于创建每个 IO shard 的跨线程命令队列。`TcpListenerOptions` 是 listener/connection 级配置，包括 `backlog`、`reuse_port`、`ipv6_only`、accept 预算、读预算、读 buffer 大小和输出高水位。

运行中可以动态增加或移除 listener。TCP server 控制面是 reactor-only：`bind_threads()`、`add_listener()`、`remove_listener()`、`start()`、`stop()` 应由同一个 reactor 线程调用；外部线程应显式投递一个 runtime task 到该 reactor。框架把这作为无锁控制面的使用契约，不为外部直接调用额外建立同步兼容层。

`start()` / 动态 `add_listener()` 表示控制任务已成功提交；真正的 `bind/listen/register channel` 在目标 IO shard 上异步完成。监听 fd 打开失败会通过 handler 的 `on_error(listener, error)` 或 `on_listener_error(listener, error)` 回调报告。这样控制面不需要 mutex、condition variable 或跨线程 barrier，也不会在 IO 线程上等待自身任务。

```cpp
const af::net::ListenerResult result = server.add_listener<MetricsHandler>({
    .name = "metrics",
    .endpoint = af::net::TcpEndpoint::loopback(9100),
});

if (result.ok()) {
    server.remove_listener(result.listener,
                           af::net::RemoveListenerPolicy::StopAcceptOnly);
}
```

`RemoveListenerPolicy::StopAcceptOnly` 只停止 accept，不影响已建立连接；`CloseExistingConnections` 会同时关闭该 listener 下已有连接。

## TCP Client API

`TcpClient` 负责主动出站连接。调用方选择绑定的 IO 线程组，`connect()` 会按配置线程轮询选择一个 IO shard，在目标 IO 线程上创建 nonblocking socket 并调用 `connect`。如果返回 `EINPROGRESS`，连接 task 通过 runtime IO wait 挂起；等待期间 IO 线程可以继续运行其他 task 和网络事件。

```cpp
af::net::TcpClient<Runtime> client;

// 在 runtime task 中执行，且后续控制操作固定在同一个 reactor 线程上。
client.bind_threads(Runtime::thread_group<IoTag>());

client.connect<ClientHandler>({
    .name = "upstream",
    .remote_endpoint = af::net::TcpEndpoint::host("127.0.0.1", 9000),
    .connect_timeout = std::chrono::seconds(3),
});
```

推荐 handler 签名：

```cpp
struct ClientHandler {
    void on_connect(af::net::TcpConnectionRef<Runtime> conn) {
        conn.send(af::Buffer::copy("hello", 5));
    }

    void on_read(af::net::TcpConnectionRef<Runtime> conn,
                 af::BufferView bytes) {}

    void on_close(af::net::TcpConnectionHandle<Runtime> conn,
                  af::net::CloseReason reason) {}

    void on_connect_error(int error) {}
};
```

TCP client 控制面与 server 一样是 reactor-only：`bind_threads()`、`connect()`、`stop()` 应由同一个 reactor 线程调用；外部线程应显式投递 runtime task。`connect()` 表示连接任务已成功提交，真实连接成功通过 `on_connect()` 回调通知，失败通过 `on_connect_error(error)` / `on_error(error)` 通知。

连接建立后不再走 client 专用热路径，而是直接复用 `TcpConnection`：读事件、写队列、跨线程 `TcpConnectionHandle::send()`、关闭和 generation 校验都与 server accepted connection 一致。`TcpClientOptions` 复用 connection 级读预算、读 buffer 和输出高水位配置，并提供 `no_delay`、`keep_alive` 开关。`TcpClient::stop()` 会在各 owner IO 线程取消尚未完成的 nonblocking connect wait，并异步回投控制线程完成 stop 状态更新，因此不会等待较长的 `connect_timeout` 或系统 TCP 超时。

`TcpClientRuntimeConfig` 是 client/shard 级配置，目前只配置跨线程 command queue 容量，避免 client API 继续暴露 server 命名。

## Unix Stream API

Unix domain stream socket 使用专门的 `UnixStreamServer` / `UnixStreamClient` API，避免用户直接把 Unix path 塞进 TCP API。底层仍然复用 `TcpServer`、`TcpClient`、`TcpConnection` 和 `TcpConnectionHandle`，所以读写、关闭、跨线程发送和 generation 校验与 TCP stream 完全一致。

```cpp
af::net::UnixStreamServer<Runtime> server;
server.bind_threads(Runtime::thread_group<IoTag>());

const auto listener = server.add_listener<UnixHandler>({
    .name = "admin",
    .endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-admin.sock"),
});

server.start();
```

Unix stream listener 强制使用 `AcceptStrategy::SingleAcceptor`：只有一个 IO shard 负责 bind/listen/accept，accepted fd 再按 stream connection 逻辑进入目标 IO shard。这样可以避免多个线程同时 bind 同一个 filesystem path。默认会在 bind 前 unlink 已存在的 path，并在 listener close/stop 后 unlink 绑定 path；可通过 `TcpListenerOptions::unlink_existing_unix_path` 和 `unlink_unix_path_on_close` 调整。

```cpp
af::net::UnixStreamClient<Runtime> client;

// 在 runtime task 中执行，且后续控制操作固定在同一个 reactor 线程上。
client.bind_threads(Runtime::thread_group<IoTag>());

client.connect<ClientHandler>({
    .name = "admin-client",
    .endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-admin.sock"),
    .connect_timeout = std::chrono::seconds(3),
});
```

## Unix Datagram API

Unix domain datagram socket 使用专门的 `UnixDatagramSocket` API。它和 IP UDP 一样是无连接 datagram 模型，但 filesystem path 的 bind/unlink 生命周期不同，所以对外不复用 `UdpSocket` 的配置入口。底层仍复用 datagram shard、MPSC command queue、读预算和跨线程发送逻辑。

```cpp
af::net::UnixDatagramSocket<Runtime> server;
server.bind_threads(Runtime::thread_group<IoTag>());

server.bind<UnixDatagramHandler>({
    .name = "unix-dgram-server",
    .local_endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-dgram.sock"),
});
```

连接式 client 可以绑定自己的本地 path，并指定 remote path：

```cpp
af::net::UnixDatagramSocket<Runtime> client;
client.bind_threads(Runtime::thread_group<IoTag>());

client.connect<ClientHandler>({
    .name = "unix-dgram-client",
    .local_endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-dgram-client.sock"),
    .remote_endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-dgram.sock"),
});

client.handle().send(af::Buffer::copy("ping", 4));
```

Unix datagram 默认在 bind 前 unlink 已存在 path，并在 stop/close 后 unlink 绑定 path；可通过 `UdpSocketOptions::unlink_existing_unix_path` 和 `unlink_unix_path_on_close` 调整。Unix datagram 只允许单个 IO shard 绑定一个 local path；如果需要多线程扩展，应显式创建多个不同 path 或在业务层做分片。

## UDP Socket API

`UdpSocket` 是 IP UDP 的统一 server/client 抽象。server 只绑定本地 IPv4/IPv6 endpoint；client 可以绑定本地 endpoint 并设置 `remote_endpoint + connect_remote=true`。每个绑定 IO shard 拥有独立 fd、handler 副本、读 buffer 和跨线程命令队列。

```cpp
af::net::UdpSocket<Runtime> server;
server.bind_threads(Runtime::thread_group<IoTag>());

server.start<UdpEchoHandler>({
    .name = "udp-echo",
    .local_endpoint = af::net::UdpEndpoint::any(9000),
    .options = {.reuse_port = true},
});
```

推荐 handler 签名：

```cpp
struct UdpEchoHandler {
    void on_datagram(af::net::UdpSocketRef<Runtime> socket,
                     af::BufferView bytes,
                     const af::net::UdpEndpoint& peer) {
        socket.send_to(bytes, peer);
    }

    void on_error(af::net::UdpSocketHandle<Runtime> socket, int error) {}
};
```

UDP client 示例：

```cpp
af::net::UdpSocket<Runtime> client;
client.bind_threads(Runtime::thread_group<IoTag>());
client.start<ClientHandler>({
    .name = "udp-client",
    .local_endpoint = af::net::UdpEndpoint::any(0),
    .remote_endpoint = af::net::UdpEndpoint::host("127.0.0.1", 9000),
    .connect_remote = true,
});

client.handle().send(af::Buffer::copy("ping", 4));
```

`UdpSocket::handle()` 在每个调用线程本地轮询 active shard，避免外部生产者默认全部集中到第一个 IO 线程，同时避免所有生产者争用一个全局原子计数器。业务需要固定亲和性时，可以启动后缓存 `handles()`，或用 `handle_for_shard()` 按业务 hash 选择目标 shard。`UdpSocketRef::send_to()` 在 IO 线程同线程发送，走直接 syscall。`UdpSocketHandle::send()` / `send_to()` 从业务线程调用时会投递到 socket 所属 IO shard 的 MPSC 队列，并唤醒该 shard。`UdpSendResult::Queued` 表示命令已入队；真正的非阻塞 send 在 IO 线程执行。

## Listener 与 Handler

TCP 每个 listener 在每个 IO shard 上拥有一份 handler 副本，避免多个 IO 线程共享 handler 状态。connection 创建时保存所属 listener context，之后读、写、关闭回调直接通过 connection 持有的 context 派发，不在热路径上查全局表。

UDP 每个 active shard 也拥有独立 handler 副本；datagram 热路径只访问本 shard 的 fd、buffer、handler，不访问全局 mutex。

UDP socket 控制面同样是 reactor-only：`bind_threads()`、`start()`、`stop()` 应由同一个 reactor 线程调用。`start()` 表示 start task 已经提交；实际 `bind/connect/register channel` 在各目标 IO shard 上完成，失败通过 handler 的 `on_error(socket, error)` 回调并异步更新 active shard 快照。

## Accept 策略

- `AcceptStrategy::Auto`：默认策略。`reuse_port=true` 时每个目标 IO 线程各自创建 listener fd；`reuse_port=false` 时只在首个 IO 线程监听，保证不会重复 bind。
- `AcceptStrategy::ReusePortPerIoThread`：强制每个目标 IO 线程创建 listener fd，要求 `reuse_port=true`。
- `AcceptStrategy::SingleAcceptor`：只在首个目标 IO 线程 accept，适合不希望或不能使用 `SO_REUSEPORT` 的场景。accepted fd 会通过 shard command 分发到绑定的 IO 线程，并在目标 IO 线程创建 `TcpConnection`。

`reuse_port=true` 是 TCP/UDP 多 IO 线程绑定同一端口的最高性能路径：内核把新连接或 datagram 分配给各 IO 线程自己的 fd，后续处理保持本地化。

## 写入策略

业务 task 要写 TCP 连接时，推荐调用 `TcpConnectionHandle::send()`。handle 内部把写请求投递到连接所属 IO 线程，由 IO 线程合并写队列、执行 syscall，并按 writable readiness 继续 flush。业务 task 不应直接跨线程操作 fd。

业务 task 要写 UDP socket 时，推荐调用 `UdpSocketHandle::send()` 或 `send_to()`。UDP datagram 不维护连接写队列，IO 线程收到命令后直接执行非阻塞 `send`/`sendto`。

## 连接管理

登录完成后，业务层可维护 `user_id -> TcpConnectionHandle`。handle 带 generation，目标 IO 线程执行写入/关闭命令时会校验 generation；连接关闭或 slot 复用后，旧 handle 不会误发到新连接。

UDP 无连接状态。业务层如果需要会话语义，可以维护 `peer endpoint -> session`，在 `on_datagram` 中解析包头、更新 session，并用 `UdpSocketHandle::send_to()` 回包。

## 性能原则

- listener、connection、UDP socket fd 尽量只在注册、interest 变化、关闭时修改 poller。
- TCP 读事件 drain 到 `EAGAIN` 或预算耗尽；UDP 按 datagram 预算 drain，避免单 fd 长时间占用 IO 线程。
- TCP 写队列按连接归属 IO 线程管理，避免多线程直接写同一 fd。
- Unix stream 复用 TCP stream connection 热路径，只有地址族、path unlink 和 accept 策略是 Unix 专属逻辑。
- Unix datagram 使用独立 API，但复用 datagram shard 热路径；单 path 只绑定一个 IO shard，避免多线程重复 bind 同一路径。
- UDP 跨线程发送使用每 shard MPSC 队列；默认 handle 轮询分散外部生产者，热路径建议业务缓存 shard handle 并按会话固定亲和性。
- `Buffer` 已支持共享底层存储的零拷贝 `slice()`、前后缀消费和 head/tail room 查询；`BufferChain` 已缓存总长度并支持跨 buffer 前缀消费。后续可继续演进为更接近 folly IOBuf 的块链、引用计数块和 prepend/append reserve 设计。
- TCP stream server/client、UDP socket、Unix stream/datagram socket 控制面属于单 reactor 线程，不使用 mutex、condition variable 或同步 barrier；accept/connect/read/write 热路径不使用全局锁。

## 后续

- 将 `Buffer`/`BufferChain` 继续演进为更适合网络包解析的分片块链，增加显式 prepend/append reserve、scatter/gather 视图和池化块复用。
