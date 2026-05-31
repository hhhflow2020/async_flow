#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_task_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void mark_ready(std::uint16_t source) noexcept {
            ready_sources_.mark(source);
        }

        void notify_external_ready() noexcept {
            if (!external_ready_.load(std::memory_order_acquire)) {
                external_ready_.store(true, std::memory_order_release);
                notify_force();
                return;
            }
            notify();
        }

        [[nodiscard]] bool try_push_local(Task* task) noexcept {
            if (local_size_ == local_queue_.size()) {
                return false;
            }

            local_queue_[local_tail_ & (local_queue_.size() - 1U)] = task;
            ++local_tail_;
            ++local_size_;
            return true;
        }

        [[nodiscard]] Task* try_pop_local() noexcept {
            if (local_size_ == 0) {
                return nullptr;
            }

            Task* task = local_queue_[local_head_ & (local_queue_.size() - 1U)];
            ++local_head_;
            --local_size_;
            return task;
        }

        void execute(Task* task) noexcept {
            TaskState expected = TaskState::Queued;
            if (!task->state_.compare_exchange_strong(
                    expected,
                    TaskState::Starting,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                AF_ASSERT(false && "executor popped a task that was not queued");
                return;
            }
            task->prepare_running_epoch();
            task->state_.store(TaskState::Running, std::memory_order_release);

            TaskResult result = TaskResult::Done;
            Task* previous_running_task = running_task_;
            running_task_ = task;
            try {
                result = task->run();
            } catch (...) {
                AF_ASSERT(false && "task::run must not throw");
                result = TaskResult::Done;
            }
            running_task_ = previous_running_task;

            switch (result) {
            case TaskResult::Done:
                finish_done(task);
                break;
            case TaskResult::Pending:
                finish_pending(task);
                break;
            case TaskResult::Again:
                finish_again(task);
                break;
            case TaskResult::Failed:
                finish_done(task);
                break;
            case TaskResult::Cancelled:
                finish_done(task);
                break;
            }
        }
