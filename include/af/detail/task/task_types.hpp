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
    // Let the runtime choose the normal low-overhead route for the producer.
    Auto,
    // Runtime-thread producer only; favors the fastest per-producer route.
    Fast,
    // Preserve one target-thread admission order across multiple producers.
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
