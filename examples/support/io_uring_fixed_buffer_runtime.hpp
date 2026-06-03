#pragma once

#include <cstddef>

#include "af/async_flow.hpp"

namespace io_uring_fixed_buffer_example {

struct FixedBufferIoThreadTag;

struct FixedBufferRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<FixedBufferIoThreadTag, 1, af::ThreadKind::IoUring, "fixed-buf">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using fixed_async = af::AsyncRuntime<FixedBufferRuntimeTraits>;
using FixedBufferTask = fixed_async::Task;
using FixedBufferThread = fixed_async::Thread;

struct FixedBufferThreads {
    static constexpr FixedBufferThread IO_0 =
        fixed_async::thread_group<FixedBufferIoThreadTag>().template at<0>();
};

struct FixedBufferRoundTripResult {
    int error{0};
    char byte_read{0};
};

} // namespace io_uring_fixed_buffer_example
