# AsyncFlow C++ 异步框架设计

本文档描述当前 AsyncFlow 的核心设计。框架采用固定线程、显式线程布局、任务状态机和 native readiness IO 后端。Linux 使用 epoll，macOS/BSD 使用 kqueue。

## 目标

- 线程数量、线程类型、线程名称由 `thread_layout` 在 traits 中声明。
- 任务由 runtime 对象池创建和销毁，业务只持有受管 task handle。
- 任务可在固定线程之间挂起和恢复，业务 key 可稳定映射到同一逻辑线程。
- runtime 线程之间使用 SPSC 队列，外部入口使用 MPSC 队列。
- IO 线程既处理 fd readiness，也处理调度到本线程的 task。
- 网络服务采用 reactor-driven 设计，fd 和连接状态归属明确的 IO 线程。
- 日志消费者绑定 runtime 线程，避免额外散落的后台线程。

## 线程模型

`thread_group<Tag, Count, Kind, Name>()` 声明一组同类线程：

- `Worker`：普通业务计算线程。
- `Log`：日志消费线程。
- `Io`：跨平台 IO 线程，Linux 映射 epoll，macOS/BSD 映射 kqueue。
- `Epoll`：Linux 显式 epoll 线程。
- `Kqueue`：macOS/BSD 显式 kqueue 线程。

`Thread` 是轻量 index，调度热路径不访问复杂对象。`thread_group` 的 `shard(key)` 只做整数映射，适合玩家、连接、房间等业务 key 分片。

## 调度模型

runtime 线程内投递：

- 目标是自身且使用快速语义时，进入 executor local queue。
- 跨 runtime 线程时，进入 source -> target 的 bounded SPSC queue。
- 使用顺序语义时，即使目标是自身，也强制走目标 MPSC queue。

外部线程投递：

- 进入目标 executor 的 bounded MPSC queue。
- 满队列行为由 `external_queue_full_policy` 控制。

`ScheduleMode::Auto` 选择默认快速路径，`Fast` 明确要求 runtime 线程快速投递，`Ordered` 明确要求顺序投递。

## Task 生命周期

任务必须通过 `Runtime::make_task<T>()` 或 `Runtime::start_task<T>()` 创建。任务构造函数接收 `Task::FactoryToken`，防止业务直接构造。

`TaskResult` 状态：

- `Done`：任务完成并释放运行引用。
- `Pending`：任务挂起，等待后续恢复。
- `Again`：任务回到当前 executor 队列继续执行。
- `Failed`：失败结束。
- `Cancelled`：取消结束。

对象池按任务类型隔离，减少跨类型竞争。跨线程释放会批量回收到 owning pool，降低远端释放频率。

## IO 模型

IO helper 和网络 reactor 都遵守固定线程归属：

- fd 在哪个 IO 线程注册，就在哪个 IO 线程处理事件。
- 业务需要跨线程处理时，显式创建 task 或 `pending_on()` 到目标逻辑线程。
- 取消 pending IO 也应调度回 fd 所属 IO 线程执行。

普通 helper 使用非阻塞 syscall + readiness wait：

- 首次调用直接尝试 `read/write/recv/send/accept/connect` 等 syscall。
- 遇到 would-block 后注册 fd readiness。
- readiness 到达后恢复原 task，在同一个 IO 线程继续执行。

Linux epoll 使用 eventfd 唤醒阻塞中的 IO executor。macOS/BSD kqueue 使用 user event 唤醒，并支持一次性 timer wait。

## 网络 Reactor

`af::net` 的目标是让 IO 线程直接运行网络事件循环：

- TCP server 绑定一个或多个 IO 线程。
- 每个连接抽象为 `TcpConnection`，由 IO 线程拥有 fd、读 buffer、写队列和状态。
- 业务通过 `TcpConnectionHandle` 管理连接引用，handle 由线程、slot、generation 组成，可避免旧引用误用新连接。
- TCP 层只提供字节流事件，packet id、长度字段、protobuf/json 解析和分发由业务层决定。
- UDP、Unix socket、client/server 复用同样的 channel/connection 抽象。

## 日志

日志前端使用 Abseil 格式化。runtime-aware sink 根据生产者来源选择队列：

- runtime 线程生产者走 SPSC lane。
- 外部线程生产者走 MPSC ingress。
- 消费者绑定到配置的 runtime 线程。
- 文件、UDP、TCP 后端都通过 runtime task 在绑定线程上执行。

## 性能原则

- 热路径避免无界队列和额外 heap 分配。
- SPSC 队列按 source/target 拆分，降低多生产者共享写指针竞争。
- MPSC 只用于外部入口和显式顺序语义。
- executor 状态按 cache line 隔离，减少 false sharing。
- IO readiness 不做无意义重复注册，interest 变化才修改内核状态。
- 业务共享状态优先按 key 分片到固定逻辑线程，少用锁。

## 当前边界

- 文件 helper 的 positioned read/write 使用绑定 IO 线程上的 POSIX syscall。
- Linux 专用 eventfd、timerfd、sendfile、splice 继续保留为 native helper。
- packet codec、dispatcher、连接管理策略不强制进入核心，业务可以按协议自行组合。
- 高阶网络协议示例可参考 `examples/net_tcp_echo_server.cpp` 和 `examples/net_tcp_login_server.cpp`。
