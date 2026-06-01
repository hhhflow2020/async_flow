#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ready_try_enqueue_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

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

    static void mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept {
        executors_[target]->mark_ready(source);
    }
