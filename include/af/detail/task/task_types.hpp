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
using shutdown_policy = ShutdownPolicy;
using task_state = TaskState;
