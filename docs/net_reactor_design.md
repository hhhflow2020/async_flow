# AsyncFlow Reactor 网络层设计

日期：2026-06-04

## 目标

新的 `af::net` 网络层以极致性能和清晰职责为目标，不再把 socket
readiness 作为普通 task 的 pending/resume 热路径。

核心目标：

- IO 热路径尽量少系统调用。
- Linux 主路径采用 epoll LT，避免 `EPOLLONESHOT` 每次事件后的 re-arm 成本。
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

Linux 主路径：

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
