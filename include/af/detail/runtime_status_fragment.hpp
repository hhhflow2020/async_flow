#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_status_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    enum class RuntimeStatus : std::uint8_t {
        Stopped,
        Starting,
        Running,
        Stopping,
    };
