#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_schedule_fragment.hpp is a task implementation fragment"
#endif

    static constexpr std::uint64_t requested_thread_mask = 0xFFFFULL;
    static constexpr std::uint64_t requested_epoch_shift = 16;
    static constexpr std::uint64_t requested_epoch_mask =
        (std::numeric_limits<std::uint64_t>::max() >> requested_epoch_shift);

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

    bool clear_requested_thread_if(std::uint64_t desired) noexcept {
        return requested_thread_.compare_exchange_strong(
            desired,
            detail::no_requested_thread,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr std::uint64_t pack_requested_thread(
        std::uint64_t epoch,
        std::uint16_t thread_index) noexcept {
        return ((epoch & requested_epoch_mask) << requested_epoch_shift) |
            (static_cast<std::uint64_t>(thread_index) + 1U);
    }

    [[nodiscard]] static constexpr std::uint64_t requested_epoch(
        std::uint64_t request) noexcept {
        return request >> requested_epoch_shift;
    }

    [[nodiscard]] static constexpr std::uint16_t requested_target(
        std::uint64_t request) noexcept {
        return static_cast<std::uint16_t>((request & requested_thread_mask) - 1U);
    }
