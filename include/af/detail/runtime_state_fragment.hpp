#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_state_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    static inline CacheLineAtomic<RuntimeStatus> status_{RuntimeStatus::Stopped};
    static inline std::array<ExternalPostCounter, thread_count> active_external_posts_{};
    static inline CacheLineAtomic<std::uint32_t> unfinished_tasks_{0};
    static inline CacheLineAtomic<std::uint64_t> generation_{0};
    static inline std::vector<std::unique_ptr<Executor>> executors_;
    static inline std::vector<std::unique_ptr<SpscQueue>> spsc_queues_;
    static inline std::vector<std::unique_ptr<ExternalQueue>> external_queues_;
    static inline std::vector<OrderedBatchState> ordered_batch_state_;
    static inline std::mutex task_registry_mutex_;
    static inline Task* task_registry_head_{nullptr};
    static inline thread_local std::uint16_t current_thread_index_ = invalid_thread_index;
