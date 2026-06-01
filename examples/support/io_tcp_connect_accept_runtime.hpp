#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_tcp_connect_accept_example {

struct TcpIoThreadTag;

#if defined(__linux__)
inline constexpr af::ThreadKind tcp_io_thread_kind = af::ThreadKind::IoUring;
#else
inline constexpr af::ThreadKind tcp_io_thread_kind = af::ThreadKind::Io;
#endif

struct TcpRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<TcpIoThreadTag, 1, tcp_io_thread_kind, "tcp-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using tcp_async = af::AsyncRuntime<TcpRuntimeTraits>;
using TcpTask = tcp_async::Task;
using TcpThread = tcp_async::Thread;

struct TcpThreads {
    static constexpr TcpThread IO_0 = tcp_async::thread_group<TcpIoThreadTag>().template at<0>();
};

[[nodiscard]] inline const char *tcp_backend_name() noexcept {
#if defined(__linux__)
    return tcp_async::io_uring_backend_available(TcpThreads::IO_0) ? "io_uring" : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_tcp_connect_accept_example
