#pragma once

#include <cstddef>

#include "af/async_flow.hpp"

namespace io_uring_openat_example {

struct OpenAtIoThreadTag;

struct OpenAtRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<OpenAtIoThreadTag, 1, af::preferred_io_thread_kind, "openat-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using openat_async = af::AsyncRuntime<OpenAtRuntimeTraits>;
using OpenAtTaskBase = openat_async::Task;
using OpenAtThread = openat_async::Thread;

struct OpenAtThreads {
    static constexpr OpenAtThread IO_0 =
        openat_async::thread_group<OpenAtIoThreadTag>().template at<0>();
};

} // namespace io_uring_openat_example
