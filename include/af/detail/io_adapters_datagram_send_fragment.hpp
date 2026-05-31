#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_datagram_send_fragment.hpp is an io_adapters implementation fragment"
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus send_to_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_to_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_zc_to_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_zc_to_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_to_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_to_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_zc_to_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_zc_to_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }
#endif
