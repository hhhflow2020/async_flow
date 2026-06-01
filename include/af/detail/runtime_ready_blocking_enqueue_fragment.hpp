#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ready_blocking_enqueue_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

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
