# 异步 IO 支持审计

## 当前支持

- Linux：epoll + eventfd。
- macOS/BSD：kqueue + user event。
- 普通 socket readiness：accept、connect、recv、send、read、write、recvmsg、sendmsg。
- 网络 reactor 抽象：TCP server/client、IPv4/IPv6 UDP socket server/client、Unix domain stream server/client、Unix domain datagram socket server/client。
- Linux native helper：eventfd、timerfd、sendfile、splice、openat2、statx、fallocate 等。
- readiness poller 默认使用 LT 语义；task 级 `io_wait()` 完成后由 runtime 删除或更新 interest，网络 channel 只在 interest 变化时更新。
- kqueue timeout wait 保持一次性 timer 语义。

## 已移除能力

复杂 ring 后端及其专属能力已经移除。当前公共 API 不再暴露 direct descriptor、registered file、registered buffer、provided buffer、内核多次完成式收包、专属 zero-copy send 等接口。

## 正确性要求

- fd 的 wait、cancel、close 必须回到所属 IO 线程执行。
- `IoOpState` 只由持有该 task 的线程推进，避免跨线程写状态。
- readiness wait 完成后恢复同一个 pending task。
- 取消 readiness 时必须从 poller 删除或更新 interest，并以 `ECANCELED` 恢复 task。
- TCP client 停止时必须在 owner IO 线程取消 pending connect wait，不能等待 `connect_timeout` 或系统 TCP 超时。
- IO helper 遇到 `EINTR` 重试，遇到 would-block 才进入 wait。

## 性能要求

- epoll/kqueue readiness 不使用 one-shot rearm；poller 修改只在注册、interest 变化、取消、完成后删除等待和关闭时发生。
- eventfd/user event 唤醒需要合并，避免每次投递都写内核对象。
- IO 线程阻塞前必须先 drain 可运行 task。
- 网络 reactor 的连接对象、buffer 和写队列按 IO 线程局部分配。
- 包解析路径优先使用 `Buffer::slice()`/`BufferChain::remove_prefix()` 这类共享块和消费视图，避免把 header/content 拆包时反复复制。

## 后续建议

- 为 epoll LT reactor 增加连接级读写预算，避免单连接长时间占用 IO 线程。
- 继续补充 TCP/UDP/Unix stream/Unix datagram socket 抽象的跨平台压力测试和异常路径测试。
- 继续把 `Buffer`/`BufferChain` 演进到显式 reserve、scatter/gather view 和池化块复用，减少 packet encode/decode 的分配和复制。
