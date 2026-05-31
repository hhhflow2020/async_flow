#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_task_accounting_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

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
