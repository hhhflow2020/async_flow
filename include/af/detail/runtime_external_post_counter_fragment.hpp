#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_external_post_counter_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    struct ExternalPostCounter {
        CacheLineAtomic<std::uint32_t> value{0};
    };
