#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_socket_lifecycle_example {

struct SocketIoThreadTag;

#if defined(__linux__)
inline constexpr af::ThreadKind socket_io_thread_kind = af::ThreadKind::IoUring;
#else
inline constexpr af::ThreadKind socket_io_thread_kind = af::ThreadKind::Io;
#endif

struct SocketRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<SocketIoThreadTag, 1, socket_io_thread_kind, "socket-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using socket_async = af::AsyncRuntime<SocketRuntimeTraits>;
using SocketTask = socket_async::Task;
using SocketThread = socket_async::Thread;

struct SocketThreads {
    static constexpr SocketThread IO_0 =
        socket_async::thread_group<SocketIoThreadTag>().template at<0>();
};

struct SocketLifecycleClientResult {
    bool ok{false};
    int error{0};
};

struct SocketLifecycleServerResult {
    bool ok{false};
    int error{0};
    std::uint16_t port{0};
};

[[nodiscard]] inline const char *socket_lifecycle_backend_name() noexcept {
#if defined(__linux__)
    return socket_async::io_uring_backend_available(SocketThreads::IO_0) ? "io_uring"
                                                                         : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_socket_lifecycle_example
