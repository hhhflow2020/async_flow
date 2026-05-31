#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_stream_send_fragment.hpp is an io_adapters implementation fragment"
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus send_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_send_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_zc_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_send_zc_some(task, this->thread_, this->fd_, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_zc_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendv_zc_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif
