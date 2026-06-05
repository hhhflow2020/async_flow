#pragma once

enum class TaskResult : std::uint8_t {
    Done,
    Pending,
    Again,
    Failed,
    Cancelled,
    done = Done,
    pending = Pending,
    again = Again,
    failed = Failed,
    cancelled = Cancelled,
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
    auto_select = Auto,
    fast = Fast,
    ordered = Ordered,
};

enum class ShutdownPolicy : std::uint8_t {
    WaitForTasks,
    StopImmediately,
    wait_for_tasks = WaitForTasks,
    stop_immediately = StopImmediately,
};

enum class TaskState : std::uint8_t {
    Created,
    Queued,
    TimerArming,
    TimerPending,
    Starting,
    Running,
    Pending,
    Done,
    created = Created,
    queued = Queued,
    timer_arming = TimerArming,
    timer_pending = TimerPending,
    starting = Starting,
    running = Running,
    pending = Pending,
    done = Done,
};

using task_result = TaskResult;
using schedule_mode = ScheduleMode;
using shutdown_policy = ShutdownPolicy;
using task_state = TaskState;
