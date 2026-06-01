#pragma once

#include <cstdint>
#include <cstddef>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace io_rpc_length_prefixed_example {

struct RpcLogicThreadTag;
struct RpcIoThreadTag;

struct RpcRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RpcLogicThreadTag, 1, af::ThreadKind::Worker, "rpc-cpu">(),
        af::thread_group<RpcIoThreadTag, 1, af::ThreadKind::IoUring, "rpc-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using rpc_async = af::AsyncRuntime<RpcRuntimeTraits>;
using RpcTask = rpc_async::Task;
using RpcThread = rpc_async::Thread;

struct RpcThreads {
    static constexpr RpcThread Logic_0 =
        rpc_async::thread_group<RpcLogicThreadTag>().template at<0>();
    static constexpr RpcThread IO_0 = rpc_async::thread_group<RpcIoThreadTag>().template at<0>();
};

#if defined(__linux__)
inline constexpr std::size_t kMaxFrameBytes = 4096;
#endif

} // namespace io_rpc_length_prefixed_example
