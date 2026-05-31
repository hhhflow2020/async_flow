#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_stream_transfer_fragment.hpp is an io_adapters implementation fragment"
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus sendfile_some(
        TaskT& task,
        int file_fd,
        IoOffset* offset,
        std::size_t count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendfile_some(
            task,
            this->thread_,
            this->fd_,
            file_fd,
            offset,
            count,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus shutdown(
        TaskT& task,
        int how,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_shutdown(task, this->thread_, this->fd_, how, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus connect(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_connect(task, this->thread_, this->fd_, address, address_size, state);
    }
