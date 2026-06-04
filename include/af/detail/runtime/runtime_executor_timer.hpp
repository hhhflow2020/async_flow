#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_timer.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::int64_t Executor<RuntimeT, TraitsT>::steady_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::timer_entry_after(const TimerEntry &left,
                                               const TimerEntry &right) noexcept {
    if (left.deadline_ns != right.deadline_ns) {
        return left.deadline_ns > right.deadline_ns;
    }
    return left.sequence > right.sequence;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int Executor<RuntimeT, TraitsT>::timer_poll_timeout_ms() const noexcept {
    if (timers_.empty()) {
        return -1;
    }

    const std::int64_t now = steady_now_ns();
    const std::int64_t deadline = timers_.front().deadline_ns;
    if (deadline <= now) {
        return 0;
    }

    const std::int64_t diff_ns = deadline - now;
    constexpr std::int64_t ns_per_ms = 1000000;
    const std::int64_t timeout_ms = (diff_ns + ns_per_ms - 1) / ns_per_ms;
    if (timeout_ms > static_cast<std::int64_t>(std::numeric_limits<int>::max())) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(timeout_ms);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::wait_for_wake_or_timer(std::uint32_t observed,
                                                                       int timeout_ms) noexcept {
    if (timeout_ms < 0) {
        wake_epoch_.wait(observed, std::memory_order_acquire);
        return wake_epoch_.load(std::memory_order_acquire) != observed;
    }
    if (timeout_ms == 0) {
        return false;
    }

    return detail::atomic_wait_value_for(wake_epoch_.value, observed,
                                         std::chrono::milliseconds(timeout_ms),
                                         std::memory_order_acquire);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::handle_inbox_task(Task *task) noexcept {
    if (task == nullptr) {
        return false;
    }

    const TaskState state = task->state_.load(std::memory_order_acquire);
    if (state == TaskState::TimerArming) {
        return arm_timer_from_inbox(task);
    }
    if (state == TaskState::Done) {
        return true;
    }

    execute(task);
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::arm_timer_from_inbox(Task *task) noexcept {
    if (!task->mark_timer_pending()) {
        return true;
    }

    RuntimeT::register_pending_task(task);
    if (push_timer(task)) {
        return true;
    }

    cancel_timer_task(task);
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::push_timer(Task *task) noexcept {
    try {
        timers_.push_back(TimerEntry{task->timer_deadline_ns_, next_timer_sequence_++, task});
        std::push_heap(timers_.begin(), timers_.end(), timer_entry_after);
        return true;
    } catch (...) {
        return false;
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::run_due_timers() noexcept {
    if (timers_.empty()) {
        return false;
    }

    bool did_work = false;
    std::size_t drained = 0;
    while (drained < timer_drain_budget && !timers_.empty()) {
        const std::int64_t now = steady_now_ns();
        if (timers_.front().deadline_ns > now) {
            break;
        }

        std::pop_heap(timers_.begin(), timers_.end(), timer_entry_after);
        TimerEntry entry = timers_.back();
        timers_.pop_back();
        ++drained;

        Task *task = entry.task;
        if (task == nullptr || !task->mark_timer_ready()) {
            continue;
        }

        did_work = true;
        execute(task);
    }
    return did_work;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::cancel_timer_task(Task *task) noexcept {
    if (task == nullptr) {
        return;
    }

    TaskState expected = TaskState::TimerPending;
    if (!task->state_.compare_exchange_strong(expected, TaskState::Done, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
        return;
    }

    task->timer_deadline_ns_ = Task::no_timer_deadline_ns;
    task->timer_mode_ = ScheduleMode::Auto;
    static_cast<void>(task->take_requested_thread());
    task->on_runtime_cancel();
    on_task_finished(task);
    task->release_lifetime_ref();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::cancel_timer_tasks() noexcept {
    for (TimerEntry &entry : timers_) {
        cancel_timer_task(entry.task);
    }
    timers_.clear();
}

} // namespace af::detail
