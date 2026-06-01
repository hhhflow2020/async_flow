#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_schedule_constants_fragment.hpp is a task implementation fragment"
#endif

    static constexpr std::uint64_t requested_thread_mask = 0xFFFFULL;
    static constexpr std::uint64_t requested_epoch_shift = 16;
    static constexpr std::uint64_t requested_epoch_mask =
        (std::numeric_limits<std::uint64_t>::max() >> requested_epoch_shift);
