#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_task_handle_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename TaskT>
    class [[nodiscard]] TaskHandle {
    public:
        TaskHandle() noexcept = default;
        explicit TaskHandle(TaskT* task) noexcept : task_(task) {}

        TaskHandle(const TaskHandle&) = delete;
        TaskHandle& operator=(const TaskHandle&) = delete;

        TaskHandle(TaskHandle&& other) noexcept : task_(std::exchange(other.task_, nullptr)) {}

        TaskHandle& operator=(TaskHandle&& other) noexcept {
            if (this != &other) {
                reset();
                task_ = std::exchange(other.task_, nullptr);
            }
            return *this;
        }

        ~TaskHandle() {
            reset();
        }

        [[nodiscard]] TaskT* get() const noexcept {
            return task_;
        }

        [[nodiscard]] TaskT& operator*() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return *task_;
        }

        [[nodiscard]] TaskT* operator->() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return task_;
        }

        [[nodiscard]] explicit operator bool() const noexcept {
            return task_ != nullptr;
        }

        [[nodiscard]] bool scheduled() const noexcept {
            return task_ != nullptr && !AsyncRuntime::is_task_created(task_);
        }

        void reset() noexcept {
            if (task_ != nullptr) {
                AsyncRuntime::release_task_handle(task_);
                task_ = nullptr;
            }
        }

    private:
        TaskT* task_{nullptr};
    };
