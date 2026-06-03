# AsyncFlow Reactor 网络层设计

日期：2026-06-04

## 目标

新的 `af::net` 网络层以极致性能和清晰职责为目标，不再把 socket
readiness 作为普通 task 的 pending/resume 热路径。

核心目标：

- IO 热路径尽量少系统调用。
- Linux native readiness 主路径采用 epoll LT，避免 `EPOLLONESHOT` 每次事件后的
  re-arm 成本；`ThreadKind::IoUring` 线程优先用 io_uring poll 承接同一套
  `NetIoChannel` readiness。
- fd、buffer、epoll interest、连接状态都由 owner IO 线程独占管理。
- 用户态内部尽量零拷贝，通过 `BufferView`、`BufferSlice`、`BufferChain`
  传递数据。
- TCP/UDP/Unix socket 的 server/client 都能在同一套 reactor/channel 抽象上扩展。
- 协议解析、packet id 分发、业务任务创建由用户掌控，框架不强制 codec 或
  dispatcher。
- IO 线程既处理网络事件，也处理调度到本线程的 task；两者通过 budget 公平推进。

## 分层

```text
af::runtime
  线程调度、task、cooperative wait、跨线程队列

af::net
  TcpServer / TcpClient / TcpConnection
  UdpServer / UdpClient / UdpEndpoint
  UnixStreamServer / UnixStreamClient / UnixStreamConnection
  UnixDatagramServer / UnixDatagramClient / UnixDatagramEndpoint

af::net::detail
  IoReactor
  IoChannel
  EpollBackend / KqueueBackend / IoUringBackend
  IO-thread command queue
  TCP/UDP/Unix endpoint pools

af::buffer
  Buffer
  BufferView
  BufferSlice
  BufferChain
  per-thread BufferPool
```

## IO 线程模型

每个 IO 线程是一个复合 executor：

```text
IO Thread
  ├── ready tasks
  ├── IO commands
  ├── reactor events
  ├── timers
  └── local/cross-thread queues
```

推荐循环：

```cpp
while (!stopping) {
    did_work |= drain_io_commands(command_budget);
    did_work |= run_ready_tasks(task_budget);
    did_work |= poll_io_events(0, event_budget);
    did_work |= run_due_timers(timer_budget);

    if (did_work) {
        continue;
    }
    if (has_ready_task() || has_io_command() || has_due_timer()) {
        continue;
    }
    poll_io_events(next_timer_timeout(), event_budget);
}
```

阻塞点只能是 `epoll_wait`、`kevent` 或 `io_uring_enter`。阻塞前必须确认
ready task、IO command、到期 timer 都为空。外部线程投递 command/task 时通过
eventfd/kqueue user event 唤醒，并需要 wake coalescing，避免大量重复 wake syscall。

## Reactor 和 Channel

`IoReactor` 归属单个 IO 线程，负责：

- 注册、更新、移除 fd。
- poll 事件并分发给 `IoChannel`。
- drain 本线程 IO command。
- 管理 wake fd 和 timer fd。

`IoChannel` 是 fd 的事件句柄，热路径不要求虚函数。Linux epoll 事件使用
`event.data.ptr = IoChannel*`，避免事件回来后再次通过 fd hash 查找。

Linux native readiness 主路径：

```text
epoll LT
listener EPOLLIN   -> accept4 loop 到 EAGAIN 或 accept budget
connection EPOLLIN -> recv loop 到 EAGAIN 或 read budget
connection EPOLLOUT -> send/writev/sendmsg loop 到 EAGAIN 或 output empty
```

`epoll_ctl` 只应发生在：

- 新 fd 注册。
- read/write interest 变化。
- fd close。

事件触发后不做 re-arm。

当前实现状态：

- `NetIoChannel` 已接入 runtime native readiness backend。
- Linux epoll 使用 LT 模式；macOS/BSD kqueue 使用持久 `EVFILT_READ/EVFILT_WRITE`，
  不使用 one-shot re-arm。
- `ThreadKind::IoUring` 线程会优先初始化 io_uring，并让 `NetIoChannel` readiness
  优先提交 `IORING_OP_POLL_ADD`。默认先尝试
  `IORING_POLL_ADD_MULTI | IORING_POLL_ADD_LEVEL`，如果内核拒绝该 flag 组合，会在同一
  IO executor 内自适应降级到 multishot poll，再降到 one-shot poll；只有 io_uring
  不可用、ring 满或 poll add 全部不可用时才自动回退到同线程 epoll LT。completion
  直接在 owner IO executor 上回调 channel。
- 当前 `af::net` TCP reactor 仍是 readiness-driven 数据面：accept/recv/send 在 owner
  IO 线程用非阻塞 syscall drain 到 `EAGAIN` 或预算耗尽。后续可在相同 shard/channel
  抽象后面继续接入 multishot accept/recv、provided buffer 和 send_zc 等 ring-native
  数据面优化。
- wake 机制在 Linux 使用 eventfd，在 kqueue/macOS 路径使用 nonblocking pipe，且有
  wake coalescing，避免每个 command 都触发一次 wake syscall。

## TCP

`TcpServer` 是用户侧控制句柄，不是普通业务 task。真正运行的是每个 IO 线程上的
`TcpServerShard`。

多 IO 线程时，Linux 默认：

```text
SO_REUSEPORT
每个 IO 线程一个 listener
每个 listener bind 同一个地址
连接从 accept 开始归属当前 IO 线程
```

`TcpConnection` 只在 owner IO 线程直接操作：

- fd
- input buffer
- output buffer
- epoll interest
- backpressure
- close state
- generation

跨线程只暴露安全句柄：

```cpp
struct TcpConnectionHandle {
    std::uint16_t io_thread;
    std::uint32_t slot;
    std::uint32_t generation;
};
```

`io_thread` 用于定位 owner IO 线程，`slot` 用于 O(1) 定位连接池槽位，
`generation` 防止旧 handle 误操作复用后的新连接。

业务 task 不直接切到 IO 线程写 fd，也不默认创建 `SendTask`。推荐：

```cpp
conn_handle.send(buffer);
```

内部生成轻量 `SendCommand` 投递到 owner IO 线程。owner IO 线程检查
`slot/generation` 后追加 output buffer，尝试发送；未写完时打开 `EPOLLOUT`。
如果当前线程就是 owner IO 线程，可走 fast path。

当前 TCP 连接控制 API：

```cpp
conn.send(bytes);
conn.close();
conn.close_after_flush();
conn.shutdown_write();
conn.pause_read();
conn.resume_read();
conn.set_no_delay(true);
conn.set_keepalive(true);
conn.local_endpoint();
conn.peer_endpoint();
conn.queued_bytes();
```

`TcpConnectionRef` 只能在 owner IO 线程回调内使用，走直接调用热路径；
`TcpConnectionHandle` 可保存到业务对象或跨线程 task 中，内部通过 bounded MPSC command
投递回连接 owner IO 线程，并用 `slot/generation` 校验连接是否仍然是同一个实例。

`TcpServer::stop()` 会为每个 shard 启动 stop task，在对应 owner IO 线程关闭 listener、
已有 connection 和 wake channel。生产服务应在收到退出信号后先调用 `server.stop()`，
再等待 runtime idle 并执行 `Runtime::shutdown()`，避免从主线程析构时跨线程
unregister reactor channel。

endpoint 支持：

- IPv4：`TcpEndpoint::any_v4(port)`、`loopback_v4(port)`、`host("127.0.0.1", port)`。
- IPv6：`TcpEndpoint::any_v6(port)`、`loopback_v6(port)`、`host("::1", port)`。
- `TcpServerOptions::ipv6_only` 控制 IPv6 listener 是否设置 `IPV6_V6ONLY`。

## 用户 API 边界

核心网络层不强制 `on_packet`、codec 或 dispatcher。

TCP 是字节流，核心回调是：

```cpp
void on_read(af::net::TcpConnectionRef conn, af::BufferView bytes);
void on_close(af::net::TcpConnectionHandle conn, af::net::CloseReason reason);
```

UDP 是 datagram，核心回调是：

```cpp
void on_datagram(af::net::UdpEndpointRef ep, af::net::DatagramView datagram);
```

常见业务协议可以由用户自己实现：

```text
packet_length + packet_id + packet_content
```

用户维护 `packet_id -> handler/task`，例如收到登录包后启动登录任务，任务切到计算
线程处理，再通过 `TcpConnectionHandle::send()` 回包。

框架可以提供可选 `af::packet` 工具，例如 stream packet parser 或 packet router，
但不进入 `TcpServer` 核心路径。

## Buffer 和零拷贝

现实边界：

- 普通 socket receive 通常仍然有一次内核到用户态 copy。
- 框架目标是在用户态内部零拷贝传递。
- 大块发送和文件发送尽量使用内核 zero-copy 能力。

数据结构：

```text
BufferView   非拥有视图
BufferSlice  引用一段 buffer，可 retain
BufferChain  多段 buffer，供 writev/sendmsg
BufferPool   每 IO 线程本地池
```

发送策略：

- 小包：合并/coalesce，减少 syscall。
- 中包：`writev` / `sendmsg`，避免拼接拷贝。
- 大包：`MSG_ZEROCOPY` 或 io_uring `send_zc`。
- 文件：`sendfile` / `splice`。
- UDP：`recvmmsg` / `sendmmsg`。

zero-copy 是策略，不是无脑默认；小包强行 zero-copy 通常更慢。

## 背压和协作等待

IO handler 不能阻塞。task 等资源时使用 cooperative wait：

```text
资源不可用
  -> task 挂到 waiter list
  -> task 返回 Pending
  -> executor 继续跑本线程其他 task/IO event
  -> 资源恢复后唤醒 task
```

内建背压：

- `input_high_watermark`：关闭 `EPOLLIN`。
- `output_high_watermark`：send 返回 backpressure。
- `max_inflight_requests`：暂停读。
- buffer pool 空：暂停读。

恢复后再打开读 interest。

## 迁移策略

本分支先新增 `af::net` handler-based 路径，并移除旧 task-driven IO
examples/tests/benchmarks。旧底层 `io_*` helper 暂时保留，直到 runtime-bound
logger、timer、filesystem 等内部依赖完成替换。

第一阶段落地顺序：

1. `Buffer` / `BufferView` / `BufferChain` / `BufferPool`。
2. `IoReactor` / `IoChannel` / epoll LT backend。
3. `TcpServer` / `TcpConnection` / `TcpConnectionHandle`。
4. send/close command、backpressure。
5. `tcp_echo_server` 示例。
6. `tcp_login_server` 示例，protobuf 仅作为示例依赖。
7. 基础 tests/benchmarks。

本阶段已经完成：

- `Buffer` / `BufferView` / `BufferChain`。
- runtime-bound `NetIoChannel`，Linux epoll LT 与 macOS/BSD kqueue LT。
- `TcpServer` / `TcpConnectionRef` / `TcpConnectionHandle`。
- IPv4/IPv6 numeric endpoint 与 `sockaddr_storage` 转换。
- send/close/flush shutdown/read pause/keepalive/nodelay command。
- `tcp_echo_server` 与 `tcp_login_server` 示例。
- buffer 与 socket address 单测，macOS kqueue IPv4/IPv6 smoke，远端 Linux 验证计划。

仍未完成、需要后续阶段继续：

- `TcpClient`、`UdpServer`、`UdpClient`、Unix stream/datagram server/client。
- 真正独立的 public `IoReactor` façade；当前 channel 先复用 runtime native IO backend。
- io_uring-native TCP server 热路径，例如 multishot accept/recv、provided buffer、send_zc。
- `BufferPool`、writev/sendmsg coalescing、文件 sendfile/splice 与大包 zero-copy 策略在
  `af::net` 核心中的整合。
