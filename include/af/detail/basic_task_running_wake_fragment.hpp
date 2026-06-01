#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_running_wake_fragment.hpp is a task implementation fragment"
#endif

    // Wake requests are tagged with the current run epoch so a late requester cannot
    // strand a task after finish_pending() has already checked the request slot.
    void prepare_running_epoch() noexcept {
        const std::uint64_t next = run_epoch_.load(std::memory_order_relaxed) + 1U;
        run_epoch_.store(next, std::memory_order_release);
    }

    detail::ScheduleRequest request_wake_while_running(std::uint16_t thread_index) noexcept {
        for (;;) {
            const std::uint64_t epoch = run_epoch_.load(std::memory_order_acquire);
            const std::uint64_t desired = pack_requested_thread(epoch, thread_index);
            std::uint64_t expected = detail::no_requested_thread;
            if (requested_thread_.compare_exchange_strong(
                    expected,
                    desired,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return resolve_running_wake_request(epoch, desired);
            }

            if (expected == desired) {
                return resolve_running_wake_request(epoch, desired);
            }

            if (requested_epoch(expected) != epoch) {
                clear_requested_thread_if(expected);
                continue;
            }

            AF_ASSERT(
                requested_target(expected) == thread_index &&
                "a running task can only register one wake-up target");
            return {detail::ScheduleAction::Rejected, TaskState::Running};
        }
    }

    std::uint16_t take_requested_thread() noexcept {
        const std::uint64_t value = requested_thread_.exchange(
            detail::no_requested_thread,
            std::memory_order_acq_rel);
        if (value == detail::no_requested_thread) {
            return Runtime::invalid_thread_index;
        }
        if (requested_epoch(value) != run_epoch_.load(std::memory_order_acquire)) {
            return Runtime::invalid_thread_index;
        }
        return requested_target(value);
    }

    detail::ScheduleRequest resolve_running_wake_request(
        std::uint64_t epoch,
        std::uint64_t desired) noexcept {
        for (;;) {
            const TaskState state = state_.load(std::memory_order_acquire);
            if (state == TaskState::Running &&
                run_epoch_.load(std::memory_order_acquire) == epoch) {
                return {detail::ScheduleAction::Deferred, state};
            }

            if (!clear_requested_thread_if(desired)) {
                return {detail::ScheduleAction::Deferred, state};
            }

            if (state == TaskState::Pending) {
                TaskState expected = TaskState::Pending;
                if (state_.compare_exchange_strong(
                        expected,
                        TaskState::Queued,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    return {detail::ScheduleAction::Enqueue, TaskState::Pending};
                }
                continue;
            }

            if (state == TaskState::Queued || state == TaskState::Starting ||
                state == TaskState::Running) {
                return {detail::ScheduleAction::Deferred, state};
            }

            AF_ASSERT(false && "task was scheduled after completion");
            return {detail::ScheduleAction::Rejected, state};
        }
    }
