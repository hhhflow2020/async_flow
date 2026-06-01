#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_datagram_example {

struct DatagramIoThreadTag;

#if defined(__linux__)
inline constexpr af::ThreadKind datagram_io_thread_kind = af::ThreadKind::IoUring;
#else
inline constexpr af::ThreadKind datagram_io_thread_kind = af::ThreadKind::Io;
#endif

struct DatagramRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<DatagramIoThreadTag, 1, datagram_io_thread_kind, "datagram-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using datagram_async = af::AsyncRuntime<DatagramRuntimeTraits>;
using DatagramTask = datagram_async::Task;
using DatagramThread = datagram_async::Thread;

struct DatagramThreads {
    static constexpr DatagramThread IO_0 =
        datagram_async::thread_group<DatagramIoThreadTag>().template at<0>();
};

[[nodiscard]] inline const char *datagram_backend_name() noexcept {
#if defined(__linux__)
    return datagram_async::io_uring_backend_available(DatagramThreads::IO_0) ? "io_uring"
                                                                             : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

} // namespace io_uring_datagram_example
