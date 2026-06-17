# 网络 Reactor 设计

> 本文记录当前网络 reactor 设计和已实现 API。下一代目标架构中，reactor 继续作为 epoll/kqueue/select 的统一抽象，线程类型只保留 `thread_kind::io` / `thread_kind::cpu`，跨线程 socket 操作通过 task 显式调度到 owner reactor，不再依赖隐藏 command queue。完整目标方案见 [next_runtime_architecture.md](next_runtime_architecture.md)。

当前网络层以 native readiness 为主路径：Linux 使用 epoll，macOS/BSD 使用 kqueue，其他 POSIX 平台 fallback 到 select，readiness 默认采用 LT 语义。IO 线程直接管理 fd、事件、buffer 和连接状态，业务 task 只负责业务计算和跨线程流程。

## 分层

- `reactor`：统一封装 epoll/kqueue/select 的注册、更新、删除、等待和唤醒。
- `fd_event_source`：保存 fd、interest、ready events、owner 和回调。
- `tcp_endpoint` / `udp_endpoint` / `unix_endpoint`：IPv4、IPv6 和 Unix path endpoint 的语义化表示。
- `tcp_listener`：监听 fd、accept 循环、listener id、监听配置和 handler 绑定。
- `tcp_connection`：连接 fd、读 buffer、写队列、关闭状态、所属 listener 和用户上下文。
- `tcp_server`：server facade 管理逻辑 listener 和生命周期；每个目标 IO 线程有自己的 shard，shard 独占 listener fd、connection table 和连接生命周期。
- `tcp_client`：在绑定 IO 线程上发起非阻塞 connect，连接成功后交给 `tcp_connection` 统一管理。
- `udp_socket`：绑定本地 IPv4/IPv6 endpoint，可选连接 remote endpoint；同一抽象同时覆盖 UDP server/client。
- `unix_stream_server` / `unix_stream_client`：面向 Unix domain stream socket 的 server/client 控制对象，复用 stream connection 热路径。
- `unix_datagram_socket`：面向 Unix domain datagram socket 的 server/client 控制对象，复用 datagram shard 热路径并管理 path 生命周期。
- `tcp_connection_handle` / `udp_socket_handle`：线程安全的业务侧句柄，跨线程发送会投递到归属 IO shard。

## IO 线程循环

单个 IO executor 的推荐顺序：

1. drain 已经调度到本线程的 task。
2. 继续 drain 统一 intrusive MPSC task inbox 中新到达的任务。
3. 非阻塞 poll 一次 fd events。
4. 如果没有 task 和事件，进入 epoll/kqueue 阻塞等待。
5. 被 eventfd/user event 唤醒后先 drain task，再处理 fd events。

这样可以避免 IO 线程长期阻塞导致同线程 task 饥饿。

网络 channel 不使用 one-shot rearm；读写事件回调负责按预算 drain 到 `EAGAIN`
或输出队列状态变化，再只在 interest 变化、取消和关闭时更新 poller。runtime 不再暴露
task 级 IO wait facade，业务 IO 通过 TCP/UDP/Unix socket 抽象进入 reactor，因此不会依赖每次事件触发后的 one-shot rearm。

select fallback 每次 poll 从当前 channel/wait 表重建 `fd_set`，用非阻塞 pipe 做跨线程唤醒；这是兼容路径，不改变 epoll/kqueue 的热路径。

## TCP Server API

`tcp_server` 不以 handler 作为模板参数。handler 绑定在 listener 上，因此同一个 server 可以管理多个监听地址和多种业务入口。

当前 public API 已收敛到 runtime-native lower_case 控制对象：`tcp_server`、`tcp_client`、`udp_socket`、`unix_stream_server`、`unix_stream_client` 和 `unix_datagram_socket`。旧模板 `TcpServer<Runtime>`、`TcpClient<Runtime>`、`UdpSocket<Runtime>` 路径已移除。

```cpp
af::net::tcp_server server(runtime);
af::net::tcp_connection_callbacks callbacks{};
callbacks.owner = &state;
callbacks.on_accept = &on_accept;
callbacks.on_read = &on_read;
callbacks.on_close = &on_close;

// 外部线程显式投递到 owner IO 线程，后续 fd 操作仍保持 reactor-affine。
runtime.post(io_thread, [&] {
    af::net::tcp_listener_config listener;
    listener.name = "public";
    listener.endpoint = af::net::tcp_endpoint::any(8080);
    listener.threads = af::net::thread_list(runtime.io_threads());
    listener.options.reuse_port = true;

    const af::net::listener_result public_listener =
        server.add_listener(std::move(listener), callbacks);
    if (public_listener.ok()) {
        server.start();
    }
});
```

`tcp_server_config` 是 server 级配置，包含默认 `connection` 配置和 `connection_close_timeout`。`connection` 会作为 listener 的默认连接配置，覆盖读 buffer、读预算、写预算、输出高水位、`TCP_NODELAY` 和 keepalive；`tcp_listener_options` 仍可在单个 listener 上覆盖这些连接热路径参数，并继续承载 `backlog`、`reuse_port`、`ipv6_only`、accept 预算和 Unix path 生命周期选项。

运行中可以动态增加或移除 listener。TCP server 控制面是 reactor-only：`add_listener()`、`remove_listener()`、`start()`、`stop()` 应由调用方显式调度到涉及对象所属的 owner IO 线程执行；外部线程应先投递一个 runtime task 到目标 reactor。`tcp_server` facade 不维护固定 control thread 状态，只保存逻辑 listener 表和 shard 列表；真正的 listener fd、connection table、retire/reap 和回调深度都在对应 IO shard 内部，shard 只由自己的 reactor 线程访问，因此不需要 mutex。

`listener.threads` 可以绑定一个或多个 IO 线程；未填写时默认展开为 `runtime.io_threads()`。多 IO 监听同一 TCP 端口时，`reuse_port=true` 且端口非 0 会走高性能路径：每个目标 IO shard 创建自己的 nonblocking listen fd，内核把连接分配到不同 shard。`reuse_port=false` 的 `auto_select` / `single_acceptor` 会归一到首个目标 IO 线程，只创建一个 listener fd；Unix domain listener 也归一到单个 owner IO 线程。当前不做 accepted fd 跨 shard 分发，避免为了次优路径引入隐藏 command queue 或跨 reactor fd 迁移复杂度。

`start()` / 动态 `add_listener()` 表示控制任务已成功提交；真正的 `bind/listen/register channel` 在目标 IO shard 上异步完成。监听 fd 打开失败会通过 handler 的 `on_error(listener, error)` 或 `on_listener_error(listener, error)` 回调报告。这样控制面不需要 mutex、condition variable 或跨线程 barrier，也不会在 IO 线程上等待自身任务。

```cpp
af::net::tcp_listener_config metrics;
metrics.name = "metrics";
metrics.endpoint = af::net::tcp_endpoint::loopback(9100);
metrics.threads = {io_thread};

const af::net::listener_result result = server.add_listener(std::move(metrics), callbacks);

if (result.ok()) {
    server.remove_listener(result.listener,
                           af::net::remove_listener_policy::stop_accept_only);
}
```

`remove_listener_policy::stop_accept_only` 只停止 accept，不影响已建立连接；`close_existing_connections` 会同时关闭该 listener 下已有连接。

`tcp_server::stop()` 会先停止 accept 并从 reactor 移除 listener，然后对活跃连接执行 `close_after_flush()`；连接自然 flush 完会立即关闭，超过 `connection_close_timeout` 后强制关闭剩余连接，避免生产退出时被慢客户端无限拖住。

## TCP Client API

`tcp_client` 负责主动出站连接。用户把控制操作显式投递到目标 IO reactor 线程；`connect()` 在该线程创建 nonblocking socket 并调用 `connect`。如果返回 `EINPROGRESS`，client 会把 fd 注册到当前线程的 reactor，等待 writable/error/hangup 事件；等待期间 IO 线程继续运行 task 和其他网络事件。

```cpp
af::net::tcp_client client(runtime);
af::net::tcp_client_callbacks callbacks{};
callbacks.owner = &client_state;
callbacks.on_connect = &client_on_connect;
callbacks.on_read = &client_on_read;
callbacks.on_close = &client_on_close;
callbacks.on_error = &client_on_error;

runtime.post(io_thread, [&] {
    af::net::tcp_client_connect_config config;
    config.name = "upstream";
    config.remote_endpoint = af::net::tcp_endpoint::host("127.0.0.1", 9000);
    config.owner_thread = io_thread;
    config.connect_timeout = std::chrono::seconds(3);
    client.connect(std::move(config), callbacks);
});
```

推荐 callback 签名：

```cpp
void client_on_connect(void *owner, af::net::tcp_connection_ref conn) noexcept {
    static_cast<void>(owner);
    static_cast<void>(conn.send(af::buffer::copy("hello", 5)));
}

void client_on_read(void *owner, af::net::tcp_connection_ref conn,
                    af::buffer_view bytes) noexcept {
    static_cast<void>(owner);
    static_cast<void>(conn);
    static_cast<void>(bytes);
}

void client_on_close(void *owner, af::net::tcp_connection_ref conn,
                     af::net::close_reason reason) noexcept;
void client_on_error(void *owner, int error) noexcept;
```

TCP client 控制面与 server 一样是 reactor-only：`connect()`、`stop()` 应由同一个 reactor 线程调用；外部线程应显式投递 runtime task。`connect()` 表示连接任务已成功提交，真实连接成功通过 `on_connect()` 回调通知，失败通过 `on_error(error)` 通知。

连接建立后直接复用 runtime-native `tcp_connection`：读事件、写队列、跨线程 `tcp_connection_handle::send()`、关闭和 generation 校验都与 server accepted connection 一致。`tcp_client_connect_config::connection` 复用 connection 级读预算、读 buffer 和输出高水位配置，并提供 `no_delay`、`keepalive` 开关。`tcp_client::stop()` 会在 owner IO 线程取消尚未完成的 nonblocking connect wait，因此不会等待较长的 `connect_timeout` 或系统 TCP 超时。

TCP stream 连接建立后的跨线程发送、关闭和控制操作已经通过 runtime task 调度到 owner reactor，不再依赖 stream 专用 command queue。

## Unix Stream API

Unix domain stream socket 使用专门的 `unix_stream_server` / `unix_stream_client` API，避免用户直接把 Unix path 塞进 TCP API。底层仍然复用 TCP stream 热路径，所以读写、关闭、跨线程发送和 generation 校验与 TCP stream 完全一致。

```cpp
af::net::unix_stream_server server(runtime);
af::net::unix_stream_callbacks callbacks{};
callbacks.owner = &state;
callbacks.on_accept = &unix_on_accept;
callbacks.on_read = &unix_on_read;
callbacks.on_close = &unix_on_close;

runtime.post(io_thread, [&] {
    af::net::unix_stream_listener_config config;
    config.name = "admin";
    config.endpoint = af::net::unix_endpoint::unix_path("/tmp/af-admin.sock");
    config.threads = {io_thread};

    const af::net::listener_result listener =
        server.add_listener(std::move(config), callbacks);
    if (listener.ok()) {
        server.start();
    }
});
```

Unix stream listener 强制使用 `tcp_accept_strategy::single_acceptor`：只有一个 IO shard 负责 bind/listen/accept，accepted fd 再按 stream connection 逻辑进入目标 IO shard。这样可以避免多个线程同时 bind 同一个 filesystem path。默认会在 bind 前 unlink 已存在的 path，并在 listener close/stop 后 unlink 绑定 path；可通过 `tcp_listener_options::unlink_existing_unix_path` 和 `unlink_unix_path_on_close` 调整。

```cpp
af::net::unix_stream_client client(runtime);
af::net::unix_stream_client_callbacks callbacks{};
callbacks.owner = &state;
callbacks.on_connect = &unix_client_on_connect;
callbacks.on_read = &unix_client_on_read;
callbacks.on_close = &unix_client_on_close;
callbacks.on_error = &unix_client_on_error;

runtime.post(io_thread, [&] {
    af::net::unix_stream_connect_config config;
    config.name = "admin-client";
    config.endpoint = af::net::unix_endpoint::unix_path("/tmp/af-admin.sock");
    config.owner_thread = io_thread;
    config.connect_timeout = std::chrono::seconds(3);
    client.connect(std::move(config), callbacks);
});
```

## Unix Datagram API

Unix domain datagram socket 使用专门的 `unix_datagram_socket` API。它和 IP UDP 一样是无连接 datagram 模型，但 filesystem path 的 bind/unlink 生命周期不同，所以对外不复用 `udp_socket` 的配置入口。底层仍复用 datagram shard、读预算和跨线程 runtime task 发送逻辑。

```cpp
af::net::unix_datagram_socket server(runtime);
af::net::unix_datagram_callbacks callbacks{};
callbacks.owner = &state;
callbacks.on_datagram = &unix_datagram_on_packet;
callbacks.on_error = &unix_datagram_on_error;

runtime.post(io_thread, [&] {
    af::net::unix_datagram_bind_config config;
    config.name = "unix-dgram-server";
    config.local_endpoint = af::net::unix_endpoint::unix_path("/tmp/af-dgram.sock");
    config.threads = {io_thread};
    server.bind(std::move(config), callbacks);
});
```

连接式 client 可以绑定自己的本地 path，并指定 remote path：

```cpp
af::net::unix_datagram_socket client(runtime);

runtime.post(io_thread, [&] {
    af::net::unix_datagram_connect_config config;
    config.name = "unix-dgram-client";
    config.local_endpoint = af::net::unix_endpoint::unix_path("/tmp/af-dgram-client.sock");
    config.remote_endpoint = af::net::unix_endpoint::unix_path("/tmp/af-dgram.sock");
    config.threads = {io_thread};
    client.connect(std::move(config), callbacks);
});

client.handle().send(af::buffer::copy("ping", 4));
```

Unix datagram 默认在 bind 前 unlink 已存在 path，并在 stop/close 后 unlink 绑定 path；可通过 `udp_socket_options::unlink_existing_unix_path` 和 `unlink_unix_path_on_close` 调整。Unix datagram 只允许单个 IO shard 绑定一个 local path；如果需要多线程扩展，应显式创建多个不同 path 或在业务层做分片。

## UDP Socket API

`udp_socket` 是 IP UDP 的统一 server/client 抽象。server 只绑定本地 IPv4/IPv6 endpoint；client 可以绑定本地 endpoint 并设置 `remote_endpoint + connect_remote=true`。每个绑定 IO shard 拥有独立 fd、handler 副本和读 buffer；跨线程发送通过 runtime task 显式调度到 owner reactor。

```cpp
af::net::udp_socket server(runtime);
af::net::udp_socket_callbacks callbacks{};
callbacks.owner = &state;
callbacks.on_datagram = &udp_echo_on_datagram;
callbacks.on_error = &udp_on_error;

runtime.post(io_thread, [&] {
    af::net::udp_socket_config config;
    config.name = "udp-echo";
    config.local_endpoint = af::net::udp_endpoint::any(9000);
    for (std::uint16_t thread : runtime.io_threads()) {
        config.threads.push_back(af::thread_ref(thread));
    }
    config.options.reuse_port = true;
    server.start(std::move(config), callbacks);
});
```

推荐 callback 签名：

```cpp
void udp_echo_on_datagram(void *owner, af::net::udp_socket_ref socket,
                          af::buffer_view bytes,
                          const af::net::udp_peer &peer) noexcept {
    static_cast<void>(owner);
    static_cast<void>(socket.send_to(bytes, peer));
}

void udp_on_error(void *owner, af::net::udp_socket_handle socket, int error) noexcept;
```

UDP client 示例：

```cpp
af::net::udp_socket client(runtime);

runtime.post(io_thread, [&] {
    af::net::udp_socket_config config;
    config.name = "udp-client";
    config.local_endpoint = af::net::udp_endpoint::any(0);
    config.remote_endpoint = af::net::udp_endpoint::host("127.0.0.1", 9000);
    config.threads = {io_thread};
    config.connect_remote = true;
    client.start(std::move(config), callbacks);
});

client.handle().send(af::buffer::copy("ping", 4));
```

`udp_socket::handle()` 在每个调用线程本地轮询 active shard，避免外部生产者默认全部集中到第一个 IO 线程，同时避免所有生产者争用一个全局原子计数器。业务需要固定亲和性时，可以启动后缓存 `handles()`，或用 `handle_for_thread()` 按业务 hash 选择目标 IO 线程。`udp_socket_ref::send_to()` 在 IO 线程同线程发送，走直接 syscall。`udp_socket_handle::send()` / `send_to()` 从业务线程调用时会把发送操作调度到 socket 所属 IO shard。`udp_send_result::queued` 表示发送操作已提交；真正的非阻塞 send 在 IO 线程执行。

## Listener 与 Handler

TCP 每个 listener 在每个 IO shard 上拥有一份 handler 副本，避免多个 IO 线程共享 handler 状态。connection 创建时保存所属 listener context，之后读、写、关闭回调直接通过 connection 持有的 context 派发，不在热路径上查全局表。

UDP 每个 active shard 也拥有独立 handler 副本；datagram 热路径只访问本 shard 的 fd、buffer、handler，不访问全局 mutex。

UDP socket 控制面同样是 reactor-only：`start()`、`stop()` 应由 reactor 线程调用，外部线程先显式 `runtime.post()` 到目标 IO 线程。`start()` 会在当前 shard 立即执行，在其他目标 IO shard 上投递启动工作；实际 `bind/connect/register source` 在各 owner IO shard 上完成，失败通过 `on_error(socket, error)` 回调报告。

## Accept 策略

- `tcp_accept_strategy::auto_select`：默认策略。`reuse_port=true` 时每个目标 IO 线程各自创建 listener fd；`reuse_port=false` 时只在首个目标 IO 线程监听，保证不会重复 bind。
- `tcp_accept_strategy::reuse_port_per_io_thread`：强制每个目标 IO 线程创建 listener fd，要求 `reuse_port=true`。
- `tcp_accept_strategy::single_acceptor`：只在首个目标 IO 线程 accept，适合不希望或不能使用 `SO_REUSEPORT` 的场景。连接也归属该 IO 线程；后续如需 accepted fd 跨 shard 分发，应作为显式能力设计，不能通过隐藏 command queue 偷偷完成。

`reuse_port=true` 是 TCP/UDP 多 IO 线程绑定同一端口的最高性能路径：内核把新连接或 datagram 分配给各 IO 线程自己的 fd，后续处理保持本地化。

## 写入策略

业务 task 要写 TCP 连接时，推荐调用 `tcp_connection_handle::send()`。handle 内部把写请求投递到连接所属 IO 线程，由 IO 线程合并写队列、执行 syscall，并按 writable readiness 继续 flush。业务 task 不应直接跨线程操作 fd。

业务 task 要写 UDP socket 时，推荐调用 `udp_socket_handle::send()` 或 `send_to()`。UDP datagram 不维护连接写队列，发送 task 到达 owner IO 线程后直接执行非阻塞 `send`/`sendto`。

## 连接管理

登录完成后，业务层可维护 `user_id -> tcp_connection_handle`。handle 带 generation，目标 IO 线程执行写入/关闭命令时会校验 generation；连接关闭或 slot 复用后，旧 handle 不会误发到新连接。

UDP 无连接状态。业务层如果需要会话语义，可以维护 `peer endpoint -> session`，在 `on_datagram` 中解析包头、更新 session，并用 `udp_socket_handle::send_to()` 回包。

## 性能原则

- listener、connection、UDP socket fd 尽量只在注册、interest 变化、关闭时修改 poller。
- TCP 读事件 drain 到 `EAGAIN` 或预算耗尽；UDP 按 datagram 预算 drain，避免单 fd 长时间占用 IO 线程。
- TCP 写队列按连接归属 IO 线程管理，避免多线程直接写同一 fd。
- Unix stream 复用 TCP stream connection 热路径，只有地址族、path unlink 和 accept 策略是 Unix 专属逻辑。
- Unix datagram 使用独立 API，但复用 datagram shard 热路径；单 path 只绑定一个 IO shard，避免多线程重复 bind 同一路径。
- UDP 跨线程发送使用 runtime task 显式调度到 owner IO shard；默认 handle 轮询分散外部生产者，热路径建议业务缓存 shard handle 并按会话固定亲和性。
- `buffer` 已使用 Folly `IOBuf` 作为底层存储，支持共享底层数据的零拷贝 `slice()`、写前 `unshareOne()`、前后缀消费和 head/tail room 查询；`buffer_chain` 已缓存总长度、支持跨 buffer 前缀消费，并通过 `fill_iobufs()` 为 TCP `sendmsg` 热路径提供 IOBuf scatter 视图。后续应继续把 prepend/append reserve 和真正的 IOBuf chain 语义补齐。
- TCP stream server/client、UDP socket、Unix stream/datagram socket 控制面属于单 reactor 线程，不使用 mutex、condition variable 或同步 barrier；accept/connect/read/write 热路径不使用全局锁。

## 后续

- 将 `buffer`/`buffer_chain` 继续演进为更适合网络包解析的分片块链，增加显式 prepend/append reserve、scatter/gather 视图和池化块复用。
