#pragma once

template <typename TraitsT> class AsyncRuntime;

namespace detail {
template <typename RuntimeT, typename TraitsT> class Executor;
} // namespace detail

enum class TaskResult : std::uint8_t {
    Done,
    Pending,
    Again,
    Failed,
    Cancelled,
};

enum class QueueFullPolicy : std::uint8_t {
    Reject,
    Yield,
};

enum class ScheduleMode : std::uint8_t {
    // Default task admission path. The next runtime architecture routes every
    // producer through the target executor intrusive MPSC inbox.
    Auto,
    // Runtime-thread producer only. This keeps call sites explicit when a task
    // relies on framework-thread-only scheduling.
    Fast,
    // Explicitly preserve one target-inbox admission order across producers.
    Ordered,
};

enum class ShutdownPolicy : std::uint8_t {
    WaitForTasks,
    StopImmediately,
};

enum class TaskState : std::uint8_t {
    Created,
    Queued,
    Starting,
    Running,
    Pending,
    Done,
};
