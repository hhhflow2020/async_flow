# 远端构建与性能验证记录

本文档记录当前分支的有效验证结论。历史上已经放弃的后端记录已移除，避免误导后续实现。

## 当前验证目标

- Linux 容器内构建 runtime、日志、网络示例和压力测试。
- macOS 本地构建并运行 kqueue 相关路径。
- 确认 TCP echo server 和 TCP login server 示例可编译、可运行。
- 确认日志 ordered/relaxed 策略的正确性测试通过。

## 当前本地结果

本地 Debug 构建通过：

- `asyncflow_net_tcp_echo_server_example`
- `asyncflow_net_tcp_login_server_example`
- `asyncflow_runtime_tests`
- `asyncflow_log_tests`
- `asyncflow_runtime_stress_tests`

本地 `ctest --test-dir build-net-debug/build/Debug --output-on-failure` 通过：

- 143/143 passed。

## 远端验证建议

远端 Linux 主机使用 `/data` 目录和项目指定容器：

```sh
ssh root@192.168.31.192 -i ~/.ssh/ssh_linkwater
```

建议在容器内执行：

```sh
cmake --build build-remote/cmake-debug --parallel 10
ctest --test-dir build-remote/cmake-debug --output-on-failure
```

## 性能观察项

- runtime hop 吞吐和 p99 延迟。
- ordered logging 在多生产者下的 MPSC 竞争。
- relaxed logging 分片队列吞吐。
- epoll reactor 在多连接下的事件 batch、读写预算和连接迁移成本。
- object pool remote release batch 对内存和吞吐的影响。
