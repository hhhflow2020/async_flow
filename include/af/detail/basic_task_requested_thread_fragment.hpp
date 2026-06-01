#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_requested_thread_fragment.hpp is a task implementation fragment"
#endif

    bool clear_requested_thread_if(std::uint64_t desired) noexcept {
        return requested_thread_.compare_exchange_strong(
            desired,
            detail::no_requested_thread,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    [[nodiscard]] static constexpr std::uint64_t pack_requested_thread(
        std::uint64_t epoch,
        std::uint16_t thread_index) noexcept {
        return ((epoch & requested_epoch_mask) << requested_epoch_shift) |
            (static_cast<std::uint64_t>(thread_index) + 1U);
    }

    [[nodiscard]] static constexpr std::uint64_t requested_epoch(
        std::uint64_t request) noexcept {
        return request >> requested_epoch_shift;
    }

    [[nodiscard]] static constexpr std::uint16_t requested_target(
        std::uint64_t request) noexcept {
        return static_cast<std::uint16_t>((request & requested_thread_mask) - 1U);
    }
