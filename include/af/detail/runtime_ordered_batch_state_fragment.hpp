#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ordered_batch_state_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    struct alignas(detail::hardware_cache_line_size) OrderedBatchState {
        std::uint64_t last_applied_batch_id{0};
    };
