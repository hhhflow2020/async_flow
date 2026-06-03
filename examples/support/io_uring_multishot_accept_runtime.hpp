#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_multishot_accept_example {

struct AcceptIoThreadTag;

struct AcceptRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<AcceptIoThreadTag, 1, af::preferred_io_thread_kind, "accept-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using accept_async = af::AsyncRuntime<AcceptRuntimeTraits>;
using AcceptTask = accept_async::Task;
using AcceptThread = accept_async::Thread;

struct AcceptThreads {
    static constexpr AcceptThread IO_0 =
        accept_async::thread_group<AcceptIoThreadTag>().template at<0>();
};

struct MultishotAcceptResult {
    int accepted_count{0};
    int error{0};
};

[[nodiscard]] inline bool multishot_accept_unsupported_error(int error) noexcept {
    return error == ENOSYS || error == EINVAL || error == EOPNOTSUPP;
}

} // namespace io_uring_multishot_accept_example
