#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_filesystem_ops_example {

struct FsIoThreadTag;

struct FsRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<FsIoThreadTag, 1, af::preferred_io_thread_kind, "fs-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
};

using fs_async = af::AsyncRuntime<FsRuntimeTraits>;
using FsTaskBase = fs_async::Task;
using FsThread = fs_async::Thread;

struct FsThreads {
    static constexpr FsThread IO_0 = fs_async::thread_group<FsIoThreadTag>().template at<0>();
};

struct FsResult {
    int error{0};
    std::uint64_t observed_size{0};
};

} // namespace io_uring_filesystem_ops_example
