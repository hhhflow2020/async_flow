#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_queue_topology_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    using SpscQueue = detail::BoundedSpscQueue<Task>;
    using ExternalQueue = detail::BoundedMpscQueue<Task>;

    static void init_queues() {
        spsc_queues_.clear();
        spsc_queues_.reserve(static_cast<std::size_t>(thread_count) * thread_count);
        for (std::uint16_t source = 0; source < thread_count; ++source) {
            for (std::uint16_t target = 0; target < thread_count; ++target) {
                static_cast<void>(target);
                spsc_queues_.push_back(std::make_unique<SpscQueue>(spsc_queue_capacity));
            }
        }

        external_queues_.clear();
        external_queues_.reserve(thread_count);
        for (std::uint16_t target = 0; target < thread_count; ++target) {
            static_cast<void>(target);
            external_queues_.push_back(std::make_unique<ExternalQueue>(external_queue_capacity));
        }
    }

    [[nodiscard]] static SpscQueue& spsc_queue(
        std::uint16_t source,
        std::uint16_t target) noexcept {
        return *spsc_queues_[static_cast<std::size_t>(source) * thread_count + target];
    }
