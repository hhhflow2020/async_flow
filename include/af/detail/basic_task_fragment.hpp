#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_fragment.hpp is a task implementation fragment"
#endif

template <typename RuntimeT>
class BasicTask {
public:
    using Runtime = RuntimeT;
    using Thread = typename Runtime::Thread;
    using DestroyFn = void (*)(BasicTask*) noexcept;

    class FactoryToken {
    public:
        FactoryToken(const FactoryToken&) noexcept = default;
        FactoryToken& operator=(const FactoryToken&) = delete;

    private:
        constexpr FactoryToken() noexcept = default;

        template <typename TraitsT>
        friend class AsyncRuntime;
    };

    BasicTask() = delete;
    BasicTask(const BasicTask&) = delete;
    BasicTask& operator=(const BasicTask&) = delete;
    virtual ~BasicTask() = default;

    static void* operator new(std::size_t) = delete;
    static void* operator new[](std::size_t) = delete;
    static void* operator new(std::size_t, std::align_val_t) = delete;
    static void* operator new[](std::size_t, std::align_val_t) = delete;

protected:
    explicit BasicTask(FactoryToken) noexcept {}

    static void operator delete(void* ptr) noexcept {
        ::operator delete(ptr);
    }

    static void operator delete[](void* ptr) noexcept {
        ::operator delete[](ptr);
    }

    static void operator delete(void* ptr, std::align_val_t align) noexcept {
        ::operator delete(ptr, align);
    }

    static void operator delete[](void* ptr, std::align_val_t align) noexcept {
        ::operator delete[](ptr, align);
    }

    [[nodiscard]] bool schedule(Thread thread) noexcept {
        return Runtime::post(thread, this);
    }

    TaskResult pending_on(Thread thread) noexcept {
        if (!Runtime::post(thread, this)) {
            return TaskResult::Cancelled;
        }
        return TaskResult::Pending;
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

    [[nodiscard]] bool wait_io(Thread thread, int fd, std::uint32_t events, IoResult* result) noexcept {
        return Runtime::io_wait(thread, fd, events, this, result);
    }

    [[nodiscard]] std::uint32_t last_parallel_failures() const noexcept {
        return last_parallel_failures_;
    }

private:
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

    static constexpr std::uint64_t requested_thread_mask = 0xFFFFULL;
    static constexpr std::uint64_t requested_epoch_shift = 16;
    static constexpr std::uint64_t requested_epoch_mask =
        (std::numeric_limits<std::uint64_t>::max() >> requested_epoch_shift);

    std::atomic<TaskState> state_{TaskState::Created};
    std::atomic<std::uint64_t> requested_thread_{detail::no_requested_thread};
    std::atomic<std::uint64_t> run_epoch_{0};
    std::atomic<std::uint32_t> lifetime_refs_{1};
    std::uint32_t last_parallel_failures_{0};
    DestroyFn destroy_fn_{nullptr};
    [[no_unique_address]] std::conditional_t<
        detail::task_registry_enabled_v<Runtime>,
        detail::TaskRegistryLinks<BasicTask>,
        detail::EmptyTaskRegistryLinks>
        registry_;

    template <typename TraitsT>
    friend class AsyncRuntime;
};
