#pragma once

template <typename RuntimeT> class BasicTask {
public:
    using Runtime = RuntimeT;
    using Thread = typename Runtime::Thread;
    using TaskId = std::uint64_t;
    using DestroyFn = void (*)(BasicTask *) noexcept;

    static constexpr TaskId invalid_task_id = 0;

    class FactoryToken {
    public:
        FactoryToken(const FactoryToken &) noexcept = default;
        FactoryToken &operator=(const FactoryToken &) = delete;

    private:
        constexpr FactoryToken() noexcept = default;

        template <typename TraitsT> friend class AsyncRuntime;
    };

    BasicTask() = delete;
    BasicTask(const BasicTask &) = delete;
    BasicTask &operator=(const BasicTask &) = delete;
    virtual ~BasicTask() = default;

    static void *operator new(std::size_t) = delete;
    static void *operator new[](std::size_t) = delete;
    static void *operator new(std::size_t, std::align_val_t) = delete;
    static void *operator new[](std::size_t, std::align_val_t) = delete;

    [[nodiscard]] TaskId task_id() const noexcept {
        return task_id_;
    }

protected:
    explicit BasicTask(FactoryToken) noexcept : task_id_(allocate_task_id()) {}

    static void operator delete(void *ptr) noexcept {
        ::operator delete(ptr);
    }

    static void operator delete[](void *ptr) noexcept {
        ::operator delete[](ptr);
    }

    static void operator delete(void *ptr, std::align_val_t align) noexcept {
        ::operator delete(ptr, align);
    }

    static void operator delete[](void *ptr, std::align_val_t align) noexcept {
        ::operator delete[](ptr, align);
    }

    [[nodiscard]] bool schedule(Thread thread, ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return Runtime::post(thread, this, mode);
    }

    [[nodiscard]] bool schedule_to(Thread thread, ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return schedule(thread, mode);
    }

    [[nodiscard]] bool schedule_fast(Thread thread) noexcept {
        return schedule(thread, ScheduleMode::Fast);
    }

    [[nodiscard]] bool schedule_to_fast(Thread thread) noexcept {
        return schedule_fast(thread);
    }

    [[nodiscard]] bool schedule_ordered(Thread thread) noexcept {
        return schedule(thread, ScheduleMode::Ordered);
    }

    [[nodiscard]] bool schedule_to_ordered(Thread thread) noexcept {
        return schedule_ordered(thread);
    }

    template <typename Rep, typename Period>
    [[nodiscard]] bool schedule_after(Thread thread, std::chrono::duration<Rep, Period> delay,
                                      ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return Runtime::post_after(thread, this, normalize_delay(delay), mode);
    }

    template <typename Rep, typename Period>
    [[nodiscard]] bool schedule_after(std::chrono::duration<Rep, Period> delay,
                                      ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return schedule_after(current_thread(), delay, mode);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool schedule_at(Thread thread, std::chrono::time_point<Clock, Duration> time,
                                   ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return Runtime::post_after(thread, this, delay_until(time), mode);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool schedule_at(std::chrono::time_point<Clock, Duration> time,
                                   ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return schedule_at(current_thread(), time, mode);
    }

    TaskResult pending_on(Thread thread, ScheduleMode mode = ScheduleMode::Auto) noexcept {
        if (!Runtime::post(thread, this, mode)) {
            return TaskResult::Cancelled;
        }
        return TaskResult::Pending;
    }

    TaskResult pending_to(Thread thread, ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return pending_on(thread, mode);
    }

    TaskResult pending_fast(Thread thread) noexcept {
        return pending_on(thread, ScheduleMode::Fast);
    }

    TaskResult pending_to_fast(Thread thread) noexcept {
        return pending_fast(thread);
    }

    TaskResult pending_ordered(Thread thread) noexcept {
        return pending_on(thread, ScheduleMode::Ordered);
    }

    TaskResult pending_to_ordered(Thread thread) noexcept {
        return pending_ordered(thread);
    }

    template <typename Rep, typename Period>
    TaskResult pending_after(Thread thread, std::chrono::duration<Rep, Period> delay,
                             ScheduleMode mode = ScheduleMode::Auto) noexcept {
        if (!Runtime::post_after(thread, this, normalize_delay(delay), mode)) {
            return TaskResult::Cancelled;
        }
        return TaskResult::Pending;
    }

    template <typename Rep, typename Period>
    TaskResult pending_after(std::chrono::duration<Rep, Period> delay,
                             ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return pending_after(current_thread(), delay, mode);
    }

    template <typename Clock, typename Duration>
    TaskResult pending_at(Thread thread, std::chrono::time_point<Clock, Duration> time,
                          ScheduleMode mode = ScheduleMode::Auto) noexcept {
        if (!Runtime::post_after(thread, this, delay_until(time), mode)) {
            return TaskResult::Cancelled;
        }
        return TaskResult::Pending;
    }

    template <typename Clock, typename Duration>
    TaskResult pending_at(std::chrono::time_point<Clock, Duration> time,
                          ScheduleMode mode = ScheduleMode::Auto) noexcept {
        return pending_at(current_thread(), time, mode);
    }

    static TaskResult done() noexcept {
        return TaskResult::Done;
    }

    static TaskResult pending() noexcept {
        return TaskResult::Pending;
    }

    static TaskResult again() noexcept {
        return TaskResult::Again;
    }

    static TaskResult failed() noexcept {
        return TaskResult::Failed;
    }

    static TaskResult cancelled() noexcept {
        return TaskResult::Cancelled;
    }

    static bool runtime_stopping() noexcept {
        return Runtime::is_stopping();
    }

    static Thread current_thread() noexcept {
        return Runtime::current_thread();
    }

    static bool is_current(Thread thread) noexcept {
        return Runtime::current_thread() == thread;
    }

    [[nodiscard]] bool wait_io(Thread thread, int fd, std::uint32_t events,
                               IoResult *result) noexcept {
        return Runtime::io_wait(thread, fd, events, this, result);
    }

    [[nodiscard]] std::uint32_t last_parallel_failures() const noexcept {
        return last_parallel_failures_;
    }

private:
    static constexpr std::uint64_t requested_thread_mask = 0xFFFFULL;
    static constexpr std::uint64_t requested_mode_shift = 16;
    static constexpr std::uint64_t requested_mode_mask = 0x3ULL;
    static constexpr std::uint64_t requested_delayed_shift = 18;
    static constexpr std::uint64_t requested_epoch_shift = 19;
    static constexpr std::uint64_t requested_epoch_mask =
        (std::numeric_limits<std::uint64_t>::max() >> requested_epoch_shift);
    static constexpr std::int64_t no_timer_deadline_ns = 0;

    virtual TaskResult run() = 0;
    virtual void on_runtime_cancel() noexcept {}

    void set_destroy_fn(DestroyFn destroy_fn) noexcept {
        destroy_fn_ = destroy_fn;
    }

    void attach_start_handle() noexcept {
        add_lifetime_ref();
    }

    void destroy_self() noexcept {
        AF_ASSERT(destroy_fn_ != nullptr);
        destroy_fn_(this);
    }

    void add_lifetime_ref() noexcept {
        lifetime_refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void release_lifetime_ref() noexcept {
        if (lifetime_refs_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            destroy_self();
        }
    }

    detail::ScheduleRequest request_schedule(std::uint16_t thread_index,
                                             ScheduleMode mode) noexcept {
        for (;;) {
            TaskState state = state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Created:
            case TaskState::Pending: {
                TaskState expected = state;
                if (state_.compare_exchange_weak(expected, TaskState::Queued,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                    return {detail::ScheduleAction::Enqueue, state};
                }
                break;
            }

            case TaskState::Starting:
                break;

            case TaskState::Running:
                return request_wake_while_running(thread_index, mode, false, no_timer_deadline_ns);

            case TaskState::TimerArming:
            case TaskState::TimerPending:
            case TaskState::Queued:
            case TaskState::Done:
                AF_ASSERT(false && "task was scheduled more than once or after completion");
                return {detail::ScheduleAction::Rejected, state};
            }
        }
    }

    detail::ScheduleRequest request_timer_schedule(std::uint16_t thread_index,
                                                   std::chrono::nanoseconds delay,
                                                   ScheduleMode mode) noexcept {
        const std::int64_t deadline_ns = timer_deadline_after(delay);
        for (;;) {
            TaskState state = state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Created:
            case TaskState::Pending: {
                timer_deadline_ns_ = deadline_ns;
                timer_mode_ = mode;
                TaskState expected = state;
                if (state_.compare_exchange_weak(expected, TaskState::TimerArming,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                    return {detail::ScheduleAction::ArmTimer, state};
                }
                break;
            }

            case TaskState::Starting:
                break;

            case TaskState::Running:
                return request_wake_while_running(thread_index, mode, true, deadline_ns);

            case TaskState::TimerArming:
            case TaskState::TimerPending:
            case TaskState::Queued:
            case TaskState::Done:
                AF_ASSERT(false && "task timer was scheduled more than once or after completion");
                return {detail::ScheduleAction::Rejected, state};
            }
        }
    }

    void cancel_schedule(TaskState previous) noexcept {
        TaskState expected = TaskState::Queued;
        [[maybe_unused]] const bool ok = state_.compare_exchange_strong(
            expected, previous, std::memory_order_acq_rel, std::memory_order_acquire);
        AF_ASSERT(ok);
    }

    void cancel_timer_schedule(TaskState previous) noexcept {
        TaskState expected = TaskState::TimerArming;
        [[maybe_unused]] const bool ok = state_.compare_exchange_strong(
            expected, previous, std::memory_order_acq_rel, std::memory_order_acquire);
        AF_ASSERT(ok);
        timer_deadline_ns_ = no_timer_deadline_ns;
        timer_mode_ = ScheduleMode::Auto;
    }

    [[nodiscard]] bool is_created() const noexcept {
        return state_.load(std::memory_order_acquire) == TaskState::Created;
    }

    [[nodiscard]] bool prepare_timer_from_pending(std::int64_t deadline_ns,
                                                  ScheduleMode mode) noexcept {
        timer_deadline_ns_ = deadline_ns;
        timer_mode_ = mode;
        TaskState expected = TaskState::Pending;
        return state_.compare_exchange_strong(expected, TaskState::TimerArming,
                                              std::memory_order_acq_rel, std::memory_order_acquire);
    }

    [[nodiscard]] bool mark_timer_pending() noexcept {
        TaskState expected = TaskState::TimerArming;
        return state_.compare_exchange_strong(expected, TaskState::TimerPending,
                                              std::memory_order_acq_rel, std::memory_order_acquire);
    }

    [[nodiscard]] bool mark_timer_ready() noexcept {
        TaskState expected = TaskState::TimerPending;
        if (state_.compare_exchange_strong(expected, TaskState::Queued, std::memory_order_acq_rel,
                                           std::memory_order_acquire)) {
            timer_deadline_ns_ = no_timer_deadline_ns;
            timer_mode_ = ScheduleMode::Auto;
            return true;
        }
        return false;
    }

    void set_last_parallel_failures(std::uint32_t failures) noexcept {
        last_parallel_failures_ = failures;
    }

    // Wake requests are tagged with the current run epoch so a late requester
    // cannot strand a task after finish_pending() has already checked the request
    // slot.
    void prepare_running_epoch() noexcept {
        const std::uint64_t next = run_epoch_.load(std::memory_order_relaxed) + 1U;
        run_epoch_.store(next, std::memory_order_release);
    }

    detail::ScheduleRequest request_wake_while_running(std::uint16_t thread_index,
                                                       ScheduleMode mode, bool delayed,
                                                       std::int64_t deadline_ns) noexcept {
        for (;;) {
            const std::uint64_t epoch = run_epoch_.load(std::memory_order_acquire);
            if (delayed) {
                requested_deadline_ns_.store(deadline_ns, std::memory_order_relaxed);
            }
            const std::uint64_t desired = pack_requested_thread(epoch, thread_index, mode, delayed);
            std::uint64_t expected = detail::no_requested_thread;
            if (requested_thread_.compare_exchange_strong(
                    expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return resolve_running_wake_request(epoch, desired);
            }

            if (expected == desired) {
                return resolve_running_wake_request(epoch, desired);
            }

            if (requested_epoch(expected) != epoch) {
                clear_requested_thread_if(expected);
                continue;
            }

            AF_ASSERT(requested_target(expected) == thread_index &&
                      requested_mode(expected) == mode && requested_delayed(expected) == delayed &&
                      "a running task can only register one wake-up target and mode");
            return {detail::ScheduleAction::Rejected, TaskState::Running};
        }
    }

    detail::RequestedSchedule take_requested_schedule() noexcept {
        const std::uint64_t value =
            requested_thread_.exchange(detail::no_requested_thread, std::memory_order_acq_rel);
        if (value == detail::no_requested_thread) {
            return {Runtime::invalid_thread_index, ScheduleMode::Auto, false, no_timer_deadline_ns};
        }
        if (requested_epoch(value) != run_epoch_.load(std::memory_order_acquire)) {
            return {Runtime::invalid_thread_index, ScheduleMode::Auto, false, no_timer_deadline_ns};
        }
        const bool delayed = requested_delayed(value);
        const std::int64_t deadline_ns =
            delayed ? requested_deadline_ns_.load(std::memory_order_acquire) : no_timer_deadline_ns;
        if (delayed) {
            requested_deadline_ns_.store(no_timer_deadline_ns, std::memory_order_relaxed);
        }
        return {requested_target(value), requested_mode(value), delayed, deadline_ns};
    }

    std::uint16_t take_requested_thread() noexcept {
        return take_requested_schedule().thread_index;
    }

    detail::ScheduleRequest resolve_running_wake_request(std::uint64_t epoch,
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
                if (requested_delayed(desired)) {
                    timer_deadline_ns_ = requested_deadline_ns_.load(std::memory_order_acquire);
                    timer_mode_ = requested_mode(desired);
                    if (state_.compare_exchange_strong(expected, TaskState::TimerArming,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
                        requested_deadline_ns_.store(no_timer_deadline_ns,
                                                     std::memory_order_relaxed);
                        return {detail::ScheduleAction::ArmTimer, TaskState::Pending};
                    }
                } else if (state_.compare_exchange_strong(expected, TaskState::Queued,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_acquire)) {
                    return {detail::ScheduleAction::Enqueue, TaskState::Pending};
                }
                continue;
            }

            if (state == TaskState::Queued || state == TaskState::TimerArming ||
                state == TaskState::TimerPending || state == TaskState::Starting ||
                state == TaskState::Running) {
                return {detail::ScheduleAction::Deferred, state};
            }

            if (state == TaskState::Done) {
                return {detail::ScheduleAction::Deferred, state};
            }

            AF_ASSERT(false && "task was scheduled after completion");
            return {detail::ScheduleAction::Rejected, state};
        }
    }

    bool clear_requested_thread_if(std::uint64_t desired) noexcept {
        return requested_thread_.compare_exchange_strong(desired, detail::no_requested_thread,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr std::uint64_t pack_requested_thread(std::uint64_t epoch,
                                                                       std::uint16_t thread_index,
                                                                       ScheduleMode mode,
                                                                       bool delayed) noexcept {
        return ((epoch & requested_epoch_mask) << requested_epoch_shift) |
               (static_cast<std::uint64_t>(delayed ? 1U : 0U) << requested_delayed_shift) |
               ((static_cast<std::uint64_t>(mode) & requested_mode_mask) << requested_mode_shift) |
               (static_cast<std::uint64_t>(thread_index) + 1U);
    }

    [[nodiscard]] static constexpr std::uint64_t requested_epoch(std::uint64_t request) noexcept {
        return request >> requested_epoch_shift;
    }

    [[nodiscard]] static constexpr std::uint16_t requested_target(std::uint64_t request) noexcept {
        return static_cast<std::uint16_t>((request & requested_thread_mask) - 1U);
    }

    [[nodiscard]] static constexpr ScheduleMode requested_mode(std::uint64_t request) noexcept {
        return static_cast<ScheduleMode>((request >> requested_mode_shift) & requested_mode_mask);
    }

    [[nodiscard]] static constexpr bool requested_delayed(std::uint64_t request) noexcept {
        return ((request >> requested_delayed_shift) & 0x1ULL) != 0U;
    }

    static TaskId allocate_task_id() noexcept {
        thread_local TaskId next_local_task_id = invalid_task_id;
        thread_local TaskId local_task_id_limit = invalid_task_id;
        if (next_local_task_id == local_task_id_limit) [[unlikely]] {
            next_local_task_id =
                next_task_id_.fetch_add(task_id_block_size, std::memory_order_relaxed);
            local_task_id_limit = next_local_task_id + task_id_block_size;
        }
        return next_local_task_id++;
    }

    template <typename Rep, typename Period>
    [[nodiscard]] static std::chrono::nanoseconds
    normalize_delay(std::chrono::duration<Rep, Period> delay) noexcept {
        if (delay <= delay.zero()) {
            return std::chrono::nanoseconds(0);
        }
        return std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] static std::chrono::nanoseconds
    delay_until(std::chrono::time_point<Clock, Duration> time) noexcept {
        const auto now = Clock::now();
        if (time <= now) {
            return std::chrono::nanoseconds(0);
        }
        return normalize_delay(time - now);
    }

    [[nodiscard]] static std::int64_t steady_now_ns() noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    [[nodiscard]] static std::int64_t
    timer_deadline_after(std::chrono::nanoseconds delay) noexcept {
        const std::int64_t now = steady_now_ns();
        const std::int64_t count = delay.count();
        if (count <= 0) {
            return now;
        }
        if (count > std::numeric_limits<std::int64_t>::max() - now) {
            return std::numeric_limits<std::int64_t>::max();
        }
        return now + count;
    }

    std::atomic<TaskState> state_{TaskState::Created};
    std::atomic<std::uint64_t> requested_thread_{detail::no_requested_thread};
    std::atomic<std::int64_t> requested_deadline_ns_{no_timer_deadline_ns};
    std::atomic<std::uint64_t> run_epoch_{0};
    std::atomic<std::uint32_t> lifetime_refs_{1};
    detail::IntrusiveMpscNode<BasicTask> intrusive_mpsc_node_{this};
    static constexpr TaskId task_id_block_size = 1024;
    alignas(detail::hardware_cache_line_size) static inline std::atomic<TaskId> next_task_id_{1};
    const TaskId task_id_;
    std::int64_t timer_deadline_ns_{no_timer_deadline_ns};
    std::uint32_t last_parallel_failures_{0};
    ScheduleMode timer_mode_{ScheduleMode::Auto};
    DestroyFn destroy_fn_{nullptr};
    [[no_unique_address]] std::conditional_t<detail::task_registry_enabled_v<Runtime>,
                                             detail::TaskRegistryLinks<BasicTask>,
                                             detail::EmptyTaskRegistryLinks> registry_;

    template <typename TraitsT> friend class AsyncRuntime;

    template <typename T> friend class detail::IntrusiveMpscQueue;

    template <typename RuntimeForExecutor, typename TraitsForRuntime> friend class detail::Executor;
};
