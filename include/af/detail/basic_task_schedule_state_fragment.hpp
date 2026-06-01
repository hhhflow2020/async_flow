#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_schedule_state_fragment.hpp is a task implementation fragment"
#endif

    detail::ScheduleRequest request_schedule(std::uint16_t thread_index) noexcept {
        for (;;) {
            TaskState state = state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Created:
            case TaskState::Pending: {
                TaskState expected = state;
                if (state_.compare_exchange_weak(
                        expected,
                        TaskState::Queued,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    return {detail::ScheduleAction::Enqueue, state};
                }
                break;
            }

            case TaskState::Starting:
                break;

            case TaskState::Running:
                return request_wake_while_running(thread_index);

            case TaskState::Queued:
            case TaskState::Done:
                AF_ASSERT(false && "task was scheduled more than once or after completion");
                return {detail::ScheduleAction::Rejected, state};
            }
        }
    }

    void cancel_schedule(TaskState previous) noexcept {
        TaskState expected = TaskState::Queued;
        [[maybe_unused]] const bool ok = state_.compare_exchange_strong(
            expected,
            previous,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
        AF_ASSERT(ok);
    }

    [[nodiscard]] bool is_created() const noexcept {
        return state_.load(std::memory_order_acquire) == TaskState::Created;
    }

    void set_last_parallel_failures(std::uint32_t failures) noexcept {
        last_parallel_failures_ = failures;
    }
