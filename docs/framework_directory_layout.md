# 框架目录布局

> 当前目录布局记录的是现有代码结构。下一代目标布局见 [next_runtime_architecture.md](next_runtime_architecture.md) 的“目录建议”。

当前目录按职责拆分：

- `include/af/detail/runtime/`：runtime 配置、executor、调度、task 生命周期、public API。
- `include/af/detail/queue/`：intrusive MPSC、bounded MPSC/MPMC 队列和 backoff。
- `include/af/detail/memory/`：对象池和连续对象存储。
- `include/af/detail/io/`：native readiness helper、socket/file/event/timer adapter。
- `include/af/detail/net/`：网络 reactor 内部 channel、socket address 等基础结构。
- `include/af/net/`：网络层公开 API。
- `include/af/detail/log/`：异步日志队列、record pool、runtime 后端。
- `examples/`：可运行示例。
- `tests/`：单元测试、压力测试和支持 fixture。
- `benchmarks/`：benchmark。
- `docs/`：设计、审计和验证记录。

## 组织原则

- public umbrella 只暴露稳定 API。
- detail 目录按模块拆分，不把多个后端揉进同一个文件。
- executor 核心只保存调度和 native poller 需要的状态。
- 平台差异放在独立 backend 文件中。
- 示例只展示推荐 API，不保留历史兼容入口。
