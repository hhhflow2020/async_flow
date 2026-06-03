#pragma once

#include <cstddef>

#include "af/async_flow.hpp"

namespace io_uring_file_example {

struct FileIoThreadTag;

struct FileRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<FileIoThreadTag, 1, af::ThreadKind::IoUring, "file-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using file_async = af::AsyncRuntime<FileRuntimeTraits>;
using FileTaskBase = file_async::Task;
using FileThread = file_async::Thread;

struct FileThreads {
    static constexpr FileThread IO_0 = file_async::thread_group<FileIoThreadTag>().template at<0>();
};

} // namespace io_uring_file_example
