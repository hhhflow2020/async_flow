#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_datagram_example {

enum class DatagramThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct DatagramRuntimeTraits {
    using Thread = DatagramThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(DatagramThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(DatagramThread thread) noexcept {
        if (thread != DatagramThread::IO_0) {
            return af::ThreadKind::Worker;
        }
#if defined(__linux__)
        return af::ThreadKind::IoUring;
#else
        return af::ThreadKind::Io;
#endif
    }
};

using datagram_async = af::AsyncRuntime<DatagramRuntimeTraits>;
using DatagramTask = datagram_async::Task;

[[nodiscard]] inline const char *datagram_backend_name() noexcept {
#if defined(__linux__)
    return datagram_async::io_uring_backend_available(DatagramThread::IO_0) ? "io_uring"
                                                                            : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_uring_datagram_example
