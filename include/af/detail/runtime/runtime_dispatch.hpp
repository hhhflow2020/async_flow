#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_dispatch.hpp must be included by async_runtime.hpp"
#endif

namespace af {

template <typename TraitsT> void AsyncRuntime<TraitsT>::init_queues() {
    spsc_queues_.clear();
    spsc_queues_.reserve(static_cast<std::size_t>(thread_count) * thread_count);
    for (std::uint16_t source = 0; source < thread_count; ++source) {
        for (std::uint16_t target = 0; target < thread_count; ++target) {
            static_cast<void>(target);
            spsc_queues_.push_back(std::make_unique<SpscQueue>(spsc_queue_capacity));
        }
    }

    external_queues_.clear();
    external_queues_.reserve(thread_count);
    for (std::uint16_t target = 0; target < thread_count; ++target) {
        static_cast<void>(target);
        external_queues_.push_back(std::make_unique<ExternalQueue>(external_queue_capacity));
    }
}

template <typename TraitsT>
auto AsyncRuntime<TraitsT>::spsc_queue(std::uint16_t source, std::uint16_t target) noexcept
    -> SpscQueue & {
    return *spsc_queues_[static_cast<std::size_t>(source) * thread_count + target];
}

template <typename TraitsT>
constexpr auto AsyncRuntime<TraitsT>::ready_route_from_runtime_thread(std::uint16_t source,
                                                                      std::uint16_t target) noexcept
    -> ReadyQueueRoute {
    return source == target ? ReadyQueueRoute::Local : ReadyQueueRoute::Spsc;
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_ready(std::uint16_t index, Task *task) noexcept {
    if (current_thread_index_ < thread_count) {
        return try_enqueue_ready_from_runtime_thread(current_thread_index_, index, task);
    }

    return try_enqueue_external_mpsc(index, task);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_ready_from_runtime_thread(std::uint16_t source,
                                                                  std::uint16_t target,
                                                                  Task *task) noexcept {
    const ReadyQueueRoute route = ready_route_from_runtime_thread(source, target);
    if (route == ReadyQueueRoute::Local) {
        return try_enqueue_local_from_runtime_thread(target, task);
    }

    AF_ASSERT(route == ReadyQueueRoute::Spsc);
    return try_enqueue_cross_thread_spsc(source, target, task);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_local_from_runtime_thread(std::uint16_t target,
                                                                  Task *task) noexcept {
    return executors_[target]->try_push_local(task);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_cross_thread_spsc(std::uint16_t source,
                                                          std::uint16_t target,
                                                          Task *task) noexcept {
    const bool ok = spsc_queue(source, target).try_push(task);
    if (ok) {
        mark_source_ready(source, target);
        executors_[target]->notify();
    }
    return ok;
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enqueue_external_mpsc(std::uint16_t target, Task *task) noexcept {
    const bool ok = external_queues_[target]->try_push(task);
    if (ok) {
        executors_[target]->notify_external_ready();
    }
    return ok;
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept {
    executors_[target]->mark_ready(source);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_ready_blocking(std::uint16_t index, Task *task) noexcept {
    if (current_thread_index_ < thread_count) {
        enqueue_ready_blocking_from_runtime_thread(current_thread_index_, index, task);
        return;
    }

    enqueue_external_mpsc_blocking(index, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_ready_blocking_from_runtime_thread(std::uint16_t source,
                                                                       std::uint16_t target,
                                                                       Task *task) noexcept {
    const ReadyQueueRoute route = ready_route_from_runtime_thread(source, target);
    if (route == ReadyQueueRoute::Local) {
        enqueue_local_from_runtime_thread_blocking(target, task);
        return;
    }

    AF_ASSERT(route == ReadyQueueRoute::Spsc);
    enqueue_cross_thread_spsc_blocking(source, target, task);
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_local_from_runtime_thread_blocking(std::uint16_t target,
                                                                       Task *task) noexcept {
    Executor &executor = *executors_[target];
    while (!executor.try_push_local(task)) {
        if (Task *ready = executor.try_pop_local()) {
            executor.execute(ready);
        } else {
            std::this_thread::yield();
        }
    }
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_cross_thread_spsc_blocking(std::uint16_t source,
                                                               std::uint16_t target,
                                                               Task *task) noexcept {
    while (!spsc_queue(source, target).try_push(task)) {
        std::this_thread::yield();
    }
    mark_source_ready(source, target);
    executors_[target]->notify();
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_external_mpsc_blocking(std::uint16_t target,
                                                           Task *task) noexcept {
    while (!external_queues_[target]->try_push(task)) {
        std::this_thread::yield();
    }
    executors_[target]->notify_external_ready();
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::post_blocking(Thread thread, Task *task) noexcept {
    const std::uint16_t index = thread_index(thread);
    AF_ASSERT(index < thread_count);

    const detail::ScheduleRequest request = task->request_schedule(index);
    switch (request.action) {
    case detail::ScheduleAction::Enqueue:
        if (request.previous == TaskState::Created) {
            on_task_started(task);
        }
        enqueue_ready_blocking(index, task);
        return;
    case detail::ScheduleAction::Deferred:
        return;
    case detail::ScheduleAction::Rejected:
        AF_ASSERT(false && "failed to schedule task");
        return;
    }
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::enqueue_ready_by_policy(std::uint16_t index, Task *task) noexcept {
    if constexpr (queue_full_policy == QueueFullPolicy::Yield) {
        enqueue_ready_blocking(index, task);
        return true;
    } else {
        return try_enqueue_ready(index, task);
    }
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::enqueue_pending_blocking(std::uint16_t index, Task *task) noexcept {
    TaskState expected = TaskState::Pending;
    if (task->state_.compare_exchange_strong(expected, TaskState::Queued, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
        enqueue_ready_blocking(index, task);
        return;
    }
    AF_ASSERT(expected == TaskState::Queued || expected == TaskState::Starting ||
              expected == TaskState::Running);
    static_cast<void>(index);
}

template <typename TraitsT>
bool AsyncRuntime<TraitsT>::try_enter_post(std::uint16_t target) noexcept {
    const RuntimeStatus status = status_.load(std::memory_order_acquire);
    if (current_thread_index_ < thread_count) {
        if (status == RuntimeStatus::Running) {
            return true;
        }

        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            return status == RuntimeStatus::Stopping;
        }

        return false;
    }

    if (status == RuntimeStatus::Running) {
        active_external_posts_[target].value.fetch_add(1, std::memory_order_acq_rel);
        if (status_.load(std::memory_order_acquire) == RuntimeStatus::Running) {
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
    if (active_external_posts_[target].value.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
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
