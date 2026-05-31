#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_dispatch_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    using SpscQueue = detail::BoundedSpscQueue<Task>;
    using ExternalQueue = detail::BoundedMpscQueue<Task>;

    static void init_queues() {
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
            return try_enqueue_ready_from(current_thread_index_, index, task);
        }

        const bool ok = external_queues_[index]->try_push(task);
        if (ok) {
            executors_[index]->notify_external_ready();
        }
        return ok;
    }

    static bool try_enqueue_ready_from(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        if (source == target) {
            return executors_[target]->try_push_local(task);
        }

        const bool ok = spsc_queue(source, target).try_push(task);
        if (ok) {
            mark_source_ready(source, target);
            executors_[target]->notify();
        }
        return ok;
    }

    static void enqueue_ready_blocking(std::uint16_t index, Task* task) noexcept {
        if (current_thread_index_ < thread_count) {
            enqueue_ready_blocking_from(current_thread_index_, index, task);
            return;
        }

        while (!external_queues_[index]->try_push(task)) {
            std::this_thread::yield();
        }
        executors_[index]->notify_external_ready();
    }

    static void enqueue_ready_blocking_from(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        if (source == target) {
            Executor& executor = *executors_[target];
            while (!executor.try_push_local(task)) {
                if (Task* ready = executor.try_pop_local()) {
                    executor.execute(ready);
                } else {
                    std::this_thread::yield();
                }
            }
            return;
        }

        while (!spsc_queue(source, target).try_push(task)) {
            std::this_thread::yield();
        }
        mark_source_ready(source, target);
        executors_[target]->notify();
    }

    [[nodiscard]] static SpscQueue& spsc_queue(
        std::uint16_t source,
        std::uint16_t target) noexcept {
        return *spsc_queues_[static_cast<std::size_t>(source) * thread_count + target];
    }

    static void mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept {
        if constexpr (thread_count <= 64U) {
            executors_[target]->mark_ready(source);
        } else {
            static_cast<void>(source);
            static_cast<void>(target);
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
        AF_ASSERT(expected == TaskState::Queued || expected == TaskState::Running);
        static_cast<void>(index);
    }

    [[nodiscard]] static bool try_enter_post(std::uint16_t target) noexcept {
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

    static void leave_post(std::uint16_t target) noexcept {
        if (current_thread_index_ < thread_count) {
            return;
        }

        AF_ASSERT(target < thread_count);
        if (active_external_posts_[target].value.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            active_external_posts_[target].value.notify_all();
        }
    }

    static void wait_for_external_posts() noexcept {
        for (auto& counter : active_external_posts_) {
            for (;;) {
                const std::uint32_t count = counter.value.load(std::memory_order_acquire);
                if (count == 0) {
                    break;
                }
                counter.value.wait(count, std::memory_order_acquire);
            }
        }
    }
