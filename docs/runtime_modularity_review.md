# Runtime 模块化审计

## 当前拆分

- `runtime_config.hpp`：traits 归一化和静态校验。
- `runtime_executor.hpp`：executor 声明和对象布局。
- `runtime_executor_lifecycle.hpp`：启动、停止、线程命名和通知。
- `runtime_executor_scheduler.hpp`：local queue、ready source、task 执行循环。
- `runtime_executor_io_backend.hpp`：IO backend 入口、wait/cancel 通用逻辑。
- `runtime_executor_epoll_backend.hpp`：Linux epoll 实现。
- `runtime_executor_kqueue_backend.hpp`：macOS/BSD kqueue 实现。
- `runtime_executor_net_channel.hpp`：网络 channel 注册、更新、注销。
- `runtime_public_io.hpp`：runtime 对外 IO helper 桥接。

## 本次收敛结果

复杂 ring 后端已经移除，executor 对象布局只保留 native poller 状态。核心头不再保存 SQ/CQ 指针、operation pool、注册资源表或专属 submit 代码。

## 模块边界

- runtime 调度和 IO 后端分离。
- public IO helper 和网络 reactor 分离。
- 网络连接生命周期不依赖普通 task pending/resume 热路径。
- 日志 consumer 通过 runtime task 绑定线程，不侵入 executor 内部。

## 后续拆分建议

- 将 `runtime_public_io.hpp` 中不再推荐的 submit stub 继续收缩为更小的兼容边界。
- 将 `io_file_*` POSIX helper 按生命周期、读写、metadata 保持独立。
- 网络 reactor 继续按 poller、channel、tcp server、tcp connection、udp socket、unix socket 拆分。
- 示例和测试保持按功能命名，避免历史后端名进入文件名。
