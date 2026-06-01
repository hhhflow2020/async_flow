#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_ready_route_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    enum class ReadyQueueRoute : std::uint8_t {
        Local,
        Spsc,
    };

    [[nodiscard]] static constexpr ReadyQueueRoute ready_route_from_runtime_thread(
        std::uint16_t source,
        std::uint16_t target) noexcept {
        return source == target ? ReadyQueueRoute::Local : ReadyQueueRoute::Spsc;
    }
