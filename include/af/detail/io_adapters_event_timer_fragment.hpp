#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_event_timer_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoEvent : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* value,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoEvent thread type must match the task runtime thread type");
        return af::io_wait_eventfd(task, this->thread_, this->fd_, value, state);
    }
};

template <typename ThreadT>
class IoTimer : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* expirations,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoTimer thread type must match the task runtime thread type");
        return af::io_wait_timerfd(task, this->thread_, this->fd_, expirations, state);
    }
};


