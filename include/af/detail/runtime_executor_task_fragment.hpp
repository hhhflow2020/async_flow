#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_task_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void mark_ready(std::uint16_t source) noexcept {
            if constexpr (thread_count <= 64U) {
                const std::uint64_t bit = 1ULL << source;
                std::uint64_t mask = ready_sources_.load(std::memory_order_acquire);
                while ((mask & bit) == 0U &&
                       !ready_sources_.compare_exchange_weak(
                           mask,
                           mask | bit,
                           std::memory_order_release,
                           std::memory_order_acquire)) {
                }
            } else {
                static_cast<void>(source);
            }
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
            const TaskState previous = task->state_.exchange(
                TaskState::Running,
                std::memory_order_acq_rel);
            AF_ASSERT(previous == TaskState::Queued);
            if (previous != TaskState::Queued) {
                return;
            }

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
