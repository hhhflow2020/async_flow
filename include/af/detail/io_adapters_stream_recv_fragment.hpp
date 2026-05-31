#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_stream_recv_fragment.hpp is an io_adapters implementation fragment"
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recv_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_multishot(
        TaskT& task,
        std::uint16_t buffer_group,
        std::uint16_t* buffer_id,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recv_multishot(
            task,
            this->thread_,
            this->fd_,
            buffer_group,
            buffer_id,
            state,
            flags);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recvv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif
