#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace io_uring_fixed_file_example {

enum class FixedFileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FixedFileRuntimeTraits {
    using Thread = FixedFileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FixedFileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(FixedFileThread thread) noexcept {
        return thread == FixedFileThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fixed_file_async = af::AsyncRuntime<FixedFileRuntimeTraits>;
using FixedFileTask = fixed_file_async::Task;

} // namespace io_uring_fixed_file_example
