#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_dispatch.hpp must be included by async_runtime.hpp"
#endif

namespace af {

template <typename TraitsT> void AsyncRuntime<TraitsT>::init_queues() {}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_ready(std::uint16_t index, Task *task,
                                              ScheduleMode mode) noexcept {
    static_cast<void>(mode);
    return try_enqueue_inbox(index, task);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_ready_from_runtime_thread(std::uint16_t source,
                                                                  std::uint16_t target, Task *task,
                                                                  ScheduleMode mode) noexcept {
    static_cast<void>(source);
    static_cast<void>(mode);
    return try_enqueue_inbox(target, task);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_inbox(std::uint16_t target, Task *task) noexcept {
    AF_ASSERT(target < executors_.size());
    if (target >= executors_.size()) {
        return false;
    }
    executors_[target]->enqueue(task);
    return true;
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_ready_blocking(std::uint16_t index, Task *task,
                                                   ScheduleMode mode) noexcept {
    static_cast<void>(mode);
    enqueue_inbox_blocking(index, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_ready_blocking_from_runtime_thread(std::uint16_t source,
                                                                       std::uint16_t target,
                                                                       Task *task,
                                                                       ScheduleMode mode) noexcept {
    static_cast<void>(source);
    static_cast<void>(mode);
    enqueue_inbox_blocking(target, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_inbox_blocking(std::uint16_t target, Task *task) noexcept {
    static_cast<void>(try_enqueue_inbox(target, task));
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::post_blocking(Thread thread, Task *task, ScheduleMode mode) noexcept {
    const std::uint16_t index = thread_index(thread);
    AF_ASSERT(index < thread_count);

    const detail::ScheduleRequest request = task->request_schedule(index, mode);
    switch (request.action) {
    case detail::ScheduleAction::Enqueue:
        if (request.previous == TaskState::Created) {
            on_task_started(task);
        }
        enqueue_inbox_blocking(index, task);
        return;
    case detail::ScheduleAction::ArmTimer:
        if (request.previous == TaskState::Created) {
            on_task_started(task);
        }
        enqueue_timer_arming_blocking(index, task);
        return;
    case detail::ScheduleAction::Deferred:
        return;
    case detail::ScheduleAction::Rejected:
        AF_ASSERT(false && "failed to schedule task");
        return;
    }
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::enqueue_ready_by_policy(std::uint16_t index, Task *task,
                                                    ScheduleMode mode) noexcept {
    static_cast<void>(mode);
    return try_enqueue_inbox(index, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_pending_blocking(std::uint16_t index, Task *task,
                                                     ScheduleMode mode) noexcept {
    TaskState expected = TaskState::Pending;
    if (task->state_.compare_exchange_strong(expected, TaskState::Queued, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
        enqueue_ready_blocking(index, task, mode);
        return;
    }
    AF_ASSERT(expected == TaskState::Queued || expected == TaskState::Starting ||
              expected == TaskState::Running || expected == TaskState::TimerArming ||
              expected == TaskState::TimerPending);
    static_cast<void>(index);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_timer_arming_blocking(std::uint16_t index,
                                                          Task *task) noexcept {
    enqueue_inbox_blocking(index, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_pending_timer_blocking(std::uint16_t index, Task *task,
                                                           std::int64_t deadline_ns,
                                                           ScheduleMode mode) noexcept {
    if (task->prepare_timer_from_pending(deadline_ns, mode)) {
        enqueue_timer_arming_blocking(index, task);
        return;
    }
    const TaskState state = task->state_.load(std::memory_order_acquire);
    AF_ASSERT(state == TaskState::Queued || state == TaskState::Starting ||
              state == TaskState::Running || state == TaskState::TimerArming ||
              state == TaskState::TimerPending || state == TaskState::Done);
    static_cast<void>(index);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enter_post(std::uint16_t target) noexcept {
    if (current_thread_index_ < thread_count) {
        const RuntimeStatus status = status_.load(std::memory_order_acquire);
        if (status == RuntimeStatus::Running) {
            return true;
        }

        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            return status == RuntimeStatus::Stopping;
        }

        return false;
    }

    const std::uint64_t generation = generation_.load(std::memory_order_acquire);
    const RuntimeStatus status = status_.load(std::memory_order_acquire);
    if (status == RuntimeStatus::Running) {
        active_external_posts_[target].value.fetch_add(1, std::memory_order_relaxed);
        if (status_.load(std::memory_order_acquire) == RuntimeStatus::Running &&
            generation_.load(std::memory_order_acquire) == generation) {
            return true;
        }

        leave_post(target);
        return false;
    }

    return false;
}

template <typename TraitsT> void AsyncRuntime<TraitsT>::leave_post(std::uint16_t target) noexcept {
    if (current_thread_index_ < thread_count) {
        return;
    }

    AF_ASSERT(target < thread_count);
    if (active_external_posts_[target].value.fetch_sub(1, std::memory_order_release) == 1U) {
        active_external_posts_[target].value.notify_all();
    }
}

template <typename TraitsT> void AsyncRuntime<TraitsT>::wait_for_external_posts() noexcept {
    for (auto &counter : active_external_posts_) {
        for (;;) {
            const std::uint32_t count = counter.value.load(std::memory_order_acquire);
            if (count == 0) {
                break;
            }
            counter.value.wait(count, std::memory_order_acquire);
        }
    }
}

} // namespace af
