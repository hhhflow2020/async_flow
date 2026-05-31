#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_protected_fragment.hpp is a task implementation fragment"
#endif

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
