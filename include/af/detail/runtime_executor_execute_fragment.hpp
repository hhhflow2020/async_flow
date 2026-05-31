#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_execute_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

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
