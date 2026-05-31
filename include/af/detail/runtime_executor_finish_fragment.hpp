#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_finish_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void finish_done(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Done, std::memory_order_release);
            on_task_finished(task);
            task->release_lifetime_ref();
        }

        void finish_pending(Task* task) noexcept {
            task->state_.store(TaskState::Pending, std::memory_order_release);
            const std::uint16_t requested = task->take_requested_thread();
            if (requested != invalid_thread_index) {
                enqueue_pending_blocking(requested, task);
            }
        }

        void finish_again(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Queued, std::memory_order_release);
            enqueue_ready_blocking_from_runtime_thread(index_, index_, task);
        }
