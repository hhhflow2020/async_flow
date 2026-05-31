#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_socket_lifecycle_example {

enum class SocketThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct SocketRuntimeTraits {
    using Thread = SocketThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SocketThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(SocketThread thread) noexcept {
        if (thread != SocketThread::IO_0) {
            return af::ThreadKind::Worker;
        }
#if defined(__linux__)
        return af::ThreadKind::IoUring;
#else
        return af::ThreadKind::Io;
#endif
    }
};

using socket_async = af::AsyncRuntime<SocketRuntimeTraits>;
using SocketTask = socket_async::Task;

struct SocketLifecycleClientResult {
    bool ok{false};
    int error{0};
};

struct SocketLifecycleServerResult {
    bool ok{false};
    int error{0};
    std::uint16_t port{0};
};

[[nodiscard]] inline const char* socket_lifecycle_backend_name() noexcept {
#if defined(__linux__)
    return socket_async::io_uring_backend_available(SocketThread::IO_0)
        ? "io_uring"
        : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_socket_lifecycle_example
