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

enum class RpcThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct RpcRuntimeTraits {
    using Thread = RpcThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(RpcThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(RpcThread thread) noexcept {
        return thread == RpcThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using rpc_async = af::AsyncRuntime<RpcRuntimeTraits>;
using RpcTask = rpc_async::Task;

#if defined(__linux__)
inline constexpr std::size_t kMaxFrameBytes = 4096;
#endif

} // namespace io_rpc_length_prefixed_example
