#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ready_enqueue_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    enum class ReadyQueueRoute : std::uint8_t {
        Local,
        Spsc,
    };

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

    static bool try_enqueue_ready(std::uint16_t index, Task* task) noexcept {
        if (current_thread_index_ < thread_count) {
            return try_enqueue_ready_from_runtime_thread(current_thread_index_, index, task);
        }

        return try_enqueue_external_mpsc(index, task);
    }

    static bool try_enqueue_ready_from_runtime_thread(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        const ReadyQueueRoute route = ready_route_from_runtime_thread(source, target);
        if (route == ReadyQueueRoute::Local) {
            return try_enqueue_local_from_runtime_thread(target, task);
        }

        AF_ASSERT(route == ReadyQueueRoute::Spsc);
        return try_enqueue_cross_thread_spsc(source, target, task);
    }

    [[nodiscard]] static constexpr ReadyQueueRoute ready_route_from_runtime_thread(
        std::uint16_t source,
        std::uint16_t target) noexcept {
        return source == target ? ReadyQueueRoute::Local : ReadyQueueRoute::Spsc;
    }

    static bool try_enqueue_local_from_runtime_thread(
        std::uint16_t target,
        Task* task) noexcept {
        return executors_[target]->try_push_local(task);
    }

    static bool try_enqueue_cross_thread_spsc(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        const bool ok = spsc_queue(source, target).try_push(task);
        if (ok) {
            mark_source_ready(source, target);
            executors_[target]->notify();
        }
        return ok;
    }

    static bool try_enqueue_external_mpsc(std::uint16_t target, Task* task) noexcept {
        const bool ok = external_queues_[target]->try_push(task);
        if (ok) {
            executors_[target]->notify_external_ready();
        }
        return ok;
    }

    static void enqueue_ready_blocking(std::uint16_t index, Task* task) noexcept {
        if (current_thread_index_ < thread_count) {
            enqueue_ready_blocking_from_runtime_thread(current_thread_index_, index, task);
            return;
        }

        enqueue_external_mpsc_blocking(index, task);
    }

    static void enqueue_ready_blocking_from_runtime_thread(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        const ReadyQueueRoute route = ready_route_from_runtime_thread(source, target);
        if (route == ReadyQueueRoute::Local) {
            enqueue_local_from_runtime_thread_blocking(target, task);
            return;
        }

        AF_ASSERT(route == ReadyQueueRoute::Spsc);
        enqueue_cross_thread_spsc_blocking(source, target, task);
    }

    static void enqueue_local_from_runtime_thread_blocking(
        std::uint16_t target,
        Task* task) noexcept {
        Executor& executor = *executors_[target];
        while (!executor.try_push_local(task)) {
            if (Task* ready = executor.try_pop_local()) {
                executor.execute(ready);
            } else {
                std::this_thread::yield();
            }
        }
    }

    static void enqueue_cross_thread_spsc_blocking(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        while (!spsc_queue(source, target).try_push(task)) {
            std::this_thread::yield();
        }
        mark_source_ready(source, target);
        executors_[target]->notify();
    }

    static void enqueue_external_mpsc_blocking(std::uint16_t target, Task* task) noexcept {
        while (!external_queues_[target]->try_push(task)) {
            std::this_thread::yield();
        }
        executors_[target]->notify_external_ready();
    }

    static void mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept {
        executors_[target]->mark_ready(source);
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
