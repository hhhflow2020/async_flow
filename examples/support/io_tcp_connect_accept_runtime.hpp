#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_tcp_connect_accept_example {

enum class TcpThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct TcpRuntimeTraits {
    using Thread = TcpThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TcpThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(TcpThread thread) noexcept {
        if (thread != TcpThread::IO_0) {
            return af::ThreadKind::Worker;
        }
#if defined(__linux__)
        return af::ThreadKind::IoUring;
#else
        return af::ThreadKind::Io;
#endif
    }
};

using tcp_async = af::AsyncRuntime<TcpRuntimeTraits>;
using TcpTask = tcp_async::Task;

[[nodiscard]] inline const char* tcp_backend_name() noexcept {
#if defined(__linux__)
    return tcp_async::io_uring_backend_available(TcpThread::IO_0)
        ? "io_uring"
        : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_tcp_connect_accept_example
