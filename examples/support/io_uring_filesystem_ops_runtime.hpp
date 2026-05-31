#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_filesystem_ops_example {

enum class FsThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FsRuntimeTraits {
    using Thread = FsThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FsThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(FsThread thread) noexcept {
        return thread == FsThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fs_async = af::AsyncRuntime<FsRuntimeTraits>;
using FsTaskBase = fs_async::Task;

struct FsResult {
    int error{0};
    std::uint64_t observed_size{0};
};

} // namespace io_uring_filesystem_ops_example
