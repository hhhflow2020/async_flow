#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_task_pool_fragment.hpp is an AsyncRuntime implementation fragment"
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
