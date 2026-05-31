#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_multishot_accept_example {

enum class AcceptThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct AcceptRuntimeTraits {
    using Thread = AcceptThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AcceptThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(AcceptThread thread) noexcept {
        return thread == AcceptThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using accept_async = af::AsyncRuntime<AcceptRuntimeTraits>;
using AcceptTask = accept_async::Task;

struct MultishotAcceptResult {
    int accepted_count{0};
    int error{0};
};

[[nodiscard]] inline bool multishot_accept_unsupported_error(int error) noexcept {
    return error == ENOSYS || error == EINVAL || error == EOPNOTSUPP;
}

} // namespace io_uring_multishot_accept_example
