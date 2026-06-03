#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_file_lifecycle_example {

struct LifecycleIoThreadTag;

struct LifecycleRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LifecycleIoThreadTag, 1, af::preferred_io_thread_kind, "lifecycle">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using lifecycle_async = af::AsyncRuntime<LifecycleRuntimeTraits>;
using LifecycleTaskBase = lifecycle_async::Task;
using LifecycleThread = lifecycle_async::Thread;

struct LifecycleThreads {
    static constexpr LifecycleThread IO_0 =
        lifecycle_async::thread_group<LifecycleIoThreadTag>().template at<0>();
};

} // namespace io_uring_file_lifecycle_example
