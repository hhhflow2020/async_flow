#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_lifecycle_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename TaskT>
    using TaskPool = detail::ObjectPool<TaskT>;

    using ParallelGroupPool = detail::ObjectPool<ParallelGroup>;

    template <typename TaskT>
    [[nodiscard]] static TaskPool<TaskT>& task_pool() {
        static TaskPool<TaskT> pool;
        return pool;
    }

    [[nodiscard]] static ParallelGroupPool& parallel_group_pool() {
        static ParallelGroupPool pool;
        return pool;
    }

    [[nodiscard]] static ParallelGroup* create_parallel_group(
        std::uint32_t target_count,
        Task* owner,
        std::uint16_t resume_thread) {
        auto* group = parallel_group_pool().create();
        group->init(target_count, owner, resume_thread);
        return group;
    }

    static void destroy_parallel_group(ParallelGroup* group) noexcept {
        parallel_group_pool().destroy(group);
    }

    template <typename TaskT, typename... Args>
    [[nodiscard]] static TaskT* allocate_task(Args&&... args) {
        auto* task = task_pool<TaskT>().create(
            typename Task::FactoryToken{},
            std::forward<Args>(args)...);
        task->set_destroy_fn(&destroy_task<TaskT>);
        return task;
    }

    template <typename TaskT>
    static void destroy_task(Task* task) noexcept {
        task_pool<TaskT>().destroy(static_cast<TaskT*>(task));
    }

    [[nodiscard]] static bool is_task_created(Task* task) noexcept {
        return task != nullptr && task->is_created();
    }

    static void release_task_handle(Task* task) noexcept {
        if (task == nullptr) {
            return;
        }

        const TaskState state = task->state_.load(std::memory_order_acquire);
        task->release_lifetime_ref();
        if (state == TaskState::Created) {
            task->release_lifetime_ref();
        }
    }

    static void reset_task_registry() noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            task_registry_head_ = nullptr;
        }
    }

    static void register_task(Task* task) noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            AF_ASSERT(!task->registry_.registered);
            task->registry_.prev = nullptr;
            task->registry_.next = task_registry_head_;
            if (task_registry_head_ != nullptr) {
                task_registry_head_->registry_.prev = task;
            }
            task_registry_head_ = task;
            task->registry_.registered = true;
        } else {
            static_cast<void>(task);
        }
    }

    static void unregister_task(Task* task) noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            if (!task->registry_.registered) {
                AF_ASSERT(false && "task was not registered");
                return;
            }

            if (task->registry_.prev != nullptr) {
                task->registry_.prev->registry_.next = task->registry_.next;
            } else {
                task_registry_head_ = task->registry_.next;
            }
            if (task->registry_.next != nullptr) {
                task->registry_.next->registry_.prev = task->registry_.prev;
            }

            task->registry_.prev = nullptr;
            task->registry_.next = nullptr;
            task->registry_.registered = false;
        } else {
            static_cast<void>(task);
        }
    }

    static void cancel_registered_tasks() noexcept {
        if constexpr (task_registry_enabled) {
            Task* task = nullptr;
            {
                std::lock_guard<std::mutex> lock(task_registry_mutex_);
                task = task_registry_head_;
                task_registry_head_ = nullptr;
            }

            while (task != nullptr) {
                Task* next = task->registry_.next;
                task->registry_.prev = nullptr;
                task->registry_.next = nullptr;
                task->registry_.registered = false;
                cancel_registered_task(task);
                task = next;
            }
        }
    }

    static void cancel_registered_task(Task* task) noexcept {
        for (;;) {
            TaskState state = task->state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Pending:
            case TaskState::Queued: {
                TaskState expected = state;
                if (task->state_.compare_exchange_weak(
                        expected,
                        TaskState::Done,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    static_cast<void>(task->take_requested_thread());
                    task->on_runtime_cancel();
                    task->release_lifetime_ref();
                    return;
                }
                break;
            }

            case TaskState::Created:
            case TaskState::Running:
                AF_ASSERT(false && "registered task cannot be cancelled in this state");
                return;

            case TaskState::Done:
                return;
            }
        }
    }

    static void on_task_started(Task* task) noexcept {
        register_task(task);
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            unfinished_tasks_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    static void on_task_finished(Task* task) noexcept {
        unregister_task(task);
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            if (unfinished_tasks_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                unfinished_tasks_.notify_all();
            }
        }
    }
