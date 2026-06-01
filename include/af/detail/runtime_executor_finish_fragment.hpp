#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_finish_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void finish_done(Task* task) noexcept {
            task->state_.store(TaskState::Done, std::memory_order_release);
            static_cast<void>(task->take_requested_thread());
            on_task_finished(task);
            task->release_lifetime_ref();
        }

        void finish_pending(Task* task) noexcept {
            task->state_.store(TaskState::Pending, std::memory_order_release);
            const std::uint16_t requested = task->take_requested_thread();
            if (requested != invalid_thread_index) {
                // A running-task wake request is converted into a real queue entry only if
                // the task is still Pending; a concurrent Pending->Queued wake wins otherwise.
                enqueue_pending_blocking(requested, task);
            }
        }

        void finish_again(Task* task) noexcept {
            task->state_.store(TaskState::Queued, std::memory_order_release);
            static_cast<void>(task->take_requested_thread());
            enqueue_ready_blocking_from_runtime_thread(index_, index_, task);
        }
