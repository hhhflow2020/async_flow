#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_shutdown_example {

struct ShutdownIoThreadTag;

struct ShutdownRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<ShutdownIoThreadTag, 1, af::preferred_io_thread_kind, "shutdown-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using shutdown_async = af::AsyncRuntime<ShutdownRuntimeTraits>;
using ShutdownTaskBase = shutdown_async::Task;
using ShutdownThread = shutdown_async::Thread;

struct ShutdownThreads {
    static constexpr ShutdownThread IO_0 =
        shutdown_async::thread_group<ShutdownIoThreadTag>().template at<0>();
};

} // namespace io_shutdown_example
