#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace io_uring_fixed_file_example {

struct FixedFileIoThreadTag;

struct FixedFileRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<FixedFileIoThreadTag, 1, af::preferred_io_thread_kind, "fixed-file">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using fixed_file_async = af::AsyncRuntime<FixedFileRuntimeTraits>;
using FixedFileTask = fixed_file_async::Task;
using FixedFileThread = fixed_file_async::Thread;

struct FixedFileThreads {
    static constexpr FixedFileThread IO_0 =
        fixed_file_async::thread_group<FixedFileIoThreadTag>().template at<0>();
};

} // namespace io_uring_fixed_file_example
