# Runtime 模块化审计

## 当前拆分

- `detail/runtime/runtime_config.hpp`：traits 归一化和静态校验。
- `runtime/config_types.hpp`、`runtime/config_resolution.hpp`：public 结构化 runtime 配置类型与解析校验入口，`runtime_config.hpp` 只保留 umbrella include。
- `runtime_executor.hpp`：executor 声明和对象布局。
- `runtime_executor_lifecycle.hpp`：启动、停止、线程命名和通知。
- `runtime_executor_scheduler.hpp`：intrusive MPSC inbox、task 执行循环。
- `runtime_executor_timer.hpp`：executor 本地 task timer heap、timer 到期执行和退出取消。
- `runtime_executor_service.hpp`：executor 通用 service task 注册、注销和轮询执行。
- `runtime_executor_io_backend.hpp`：IO backend 入口、wait/cancel 通用逻辑。
- `runtime_executor_epoll_backend.hpp`：Linux epoll 实现。
- `runtime_executor_kqueue_backend.hpp`：macOS/BSD kqueue 实现。
- `runtime_executor_net_channel.hpp`：网络 channel 注册、更新、注销。
- `runtime_public_io.hpp`：runtime 对外 IO helper 桥接。

## 本次收敛结果

复杂 ring 后端已经移除，executor 对象布局只保留 native poller 状态。核心头不再保存 SQ/CQ 指针、operation pool、注册资源表或专属 submit 代码。`runtime_public_io.hpp` 当前只负责 IO wait、cancel 和 timer wait 的公开桥接，不再保留 submit stub。

task timer 已从 scheduler 主循环中拆到 `runtime_executor_timer.hpp`。scheduler 只编排 drain inbox、run due timers、poll/park 的顺序，timer heap 的入堆、出堆、StopImmediately 取消都在 timer 模块内。

通用 service task 已从 scheduler 主循环中拆到 `runtime_executor_service.hpp`。executor 只保存 `RuntimeServiceTask*` 列表并按预算调用 `run_service()`；service 自身负责 pending 状态和内部队列，跨线程 producer 通过 `wake_service_tasks()` 唤醒 executor。runtime async logger 现在就是一个 service task，消费热路径不再进入 task 状态机。

## 模块边界

- runtime 调度和 IO 后端分离。
- public IO helper 和网络 reactor 分离。
- 网络连接生命周期不依赖普通 task pending/resume 热路径。
- 日志 consumer 通过通用 service task 绑定 runtime 线程，不再使用长期 runtime task 驱动消费；注册/注销仍通过短 control task 在 owner executor 上完成。
- `make_task<T>()` 和 `try_make_task<T>()` 共享 task pool 生命周期管理；可恢复创建失败路径不影响普通调度热路径。
- service task 注册/注销要求在 owner runtime 线程执行，列表不加锁；跨线程唤醒不修改列表。

## 后续拆分建议

- 将 `io_file_*` POSIX helper 按生命周期、读写、metadata 保持独立。
- 网络 reactor 继续按 poller、channel、tcp server、tcp connection、udp socket、unix socket 拆分。
- 示例和测试保持按功能命名，避免历史后端名进入文件名。
