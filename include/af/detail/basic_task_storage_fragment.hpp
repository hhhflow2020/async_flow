#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_storage_fragment.hpp is a task implementation fragment"
#endif

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
