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
    // Normal low-overhead route. Runtime producers use local queue or
    // per-source SPSC links; external producers use the target MPSC queue.
    Auto,
    // Runtime-thread producer only; same low-overhead routing as Auto, but
    // rejects external producers instead of falling back to MPSC.
    Fast,
    // Force the target MPSC queue, including from runtime threads. Use this
    // when multiple producers must share one target-thread admission order.
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
