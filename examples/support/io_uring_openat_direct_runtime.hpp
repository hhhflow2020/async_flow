#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_openat_direct_example {

struct DirectOpenIoThreadTag;

struct DirectOpenRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<DirectOpenIoThreadTag, 1, af::ThreadKind::IoUring, "direct-open">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using direct_open_async = af::AsyncRuntime<DirectOpenRuntimeTraits>;
using DirectOpenTask = direct_open_async::Task;
using DirectOpenThread = direct_open_async::Thread;

struct DirectOpenThreads {
    static constexpr DirectOpenThread IO_0 =
        direct_open_async::thread_group<DirectOpenIoThreadTag>().template at<0>();
};

struct DirectOpenRoundTripResult {
    int error{0};
    char byte_read{0};
};

[[nodiscard]] inline bool unsupported_direct_open_error(int error) noexcept {
    return error == EINVAL || error == EBADF || error == ENOSYS || error == ENXIO
#ifdef EOPNOTSUPP
           || error == EOPNOTSUPP
#endif
        ;
}

} // namespace io_uring_openat_direct_example
