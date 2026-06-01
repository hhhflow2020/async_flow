#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ready_post_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    static void post_blocking(Thread thread, Task* task) noexcept {
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

    static bool enqueue_ready_by_policy(std::uint16_t index, Task* task) noexcept {
        if constexpr (queue_full_policy == QueueFullPolicy::Yield) {
            enqueue_ready_blocking(index, task);
            return true;
        } else {
            return try_enqueue_ready(index, task);
        }
    }

    static void enqueue_pending_blocking(std::uint16_t index, Task* task) noexcept {
        TaskState expected = TaskState::Pending;
        if (task->state_.compare_exchange_strong(
                expected,
                TaskState::Queued,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            enqueue_ready_blocking(index, task);
            return;
        }
        AF_ASSERT(
            expected == TaskState::Queued || expected == TaskState::Starting ||
            expected == TaskState::Running);
        static_cast<void>(index);
    }
