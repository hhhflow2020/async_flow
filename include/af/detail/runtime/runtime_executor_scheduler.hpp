#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_scheduler.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::enqueue(Task *task) noexcept {
    inbox_.push(task);
    notify();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] typename Executor<RuntimeT, TraitsT>::Task *
Executor<RuntimeT, TraitsT>::try_pop_inbox() noexcept {
    return inbox_.try_pop();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::execute(Task *task) noexcept {
    TaskState expected = TaskState::Queued;
    if (!task->state_.compare_exchange_strong(
            expected, TaskState::Starting, std::memory_order_acq_rel, std::memory_order_acquire)) {
        AF_ASSERT(false && "executor popped a task that was not queued");
        return;
    }
    task->prepare_running_epoch();
    task->state_.store(TaskState::Running, std::memory_order_release);

    TaskResult result = TaskResult::Done;
    Task *previous_running_task = running_task_;
    const auto previous_task_id = RuntimeT::current_task_id_;
    running_task_ = task;
    RuntimeT::current_task_id_ = task->task_id();
    try {
        result = task->run();
    } catch (...) {
        AF_ASSERT(false && "task::run must not throw");
        result = TaskResult::Done;
    }
    running_task_ = previous_running_task;

    switch (result) {
    case TaskResult::Done:
        finish_done(task);
        break;
    case TaskResult::Pending:
        finish_pending(task);
        break;
    case TaskResult::Again:
        finish_again(task);
        break;
    case TaskResult::Failed:
        finish_done(task);
        break;
    case TaskResult::Cancelled:
        finish_done(task);
        break;
    }
    RuntimeT::current_task_id_ = previous_task_id;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::notify_force() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (notify_native_io_backend()) {
        return;
    }
    wake_epoch_.notify_one();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::run_loop() noexcept {
    RuntimeT::current_thread_index_ = index_;
    RuntimeT::current_task_id_ = RuntimeT::invalid_task_id;

    for (;;) {
        bool did_work = false;
        while (Task *task = pop_one()) {
            did_work = true;
            execute(task);
        }

        if (stop_requested_.load(std::memory_order_acquire)) {
            if (!did_work) {
                break;
            }
            continue;
        }

        if (poll_io(0)) {
            continue;
        }

        const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
        sleeping_.store(true, std::memory_order_release);
        if (stop_requested_.load(std::memory_order_acquire)) {
            sleeping_.store(false, std::memory_order_relaxed);
            continue;
        }

        if (Task *task = pop_one()) {
            sleeping_.store(false, std::memory_order_relaxed);
            execute(task);
        } else {
            if (wake_epoch_.load(std::memory_order_acquire) != observed) {
                sleeping_.store(false, std::memory_order_relaxed);
                continue;
            }
            if (io_thread() && io_backend_available()) {
                static_cast<void>(poll_io(-1));
            } else {
                wake_epoch_.wait(observed, std::memory_order_acquire);
            }
            sleeping_.store(false, std::memory_order_relaxed);
        }
    }

    RuntimeT::current_thread_index_ = invalid_thread_index;
    RuntimeT::current_task_id_ = RuntimeT::invalid_task_id;
}

template <typename RuntimeT, typename TraitsT>
typename Executor<RuntimeT, TraitsT>::Task *Executor<RuntimeT, TraitsT>::pop_one() noexcept {
    return try_pop_inbox();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_done(Task *task) noexcept {
    task->state_.store(TaskState::Done, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    on_task_finished(task);
    task->release_lifetime_ref();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_pending(Task *task) noexcept {
    task->state_.store(TaskState::Pending, std::memory_order_release);
    const detail::RequestedSchedule requested = task->take_requested_schedule();
    if (requested.thread_index != invalid_thread_index) {
        enqueue_pending_blocking(requested.thread_index, task, requested.mode);
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_again(Task *task) noexcept {
    task->state_.store(TaskState::Queued, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    enqueue_ready_blocking_from_runtime_thread(index_, index_, task, ScheduleMode::Auto);
}

} // namespace af::detail
