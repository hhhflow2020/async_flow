#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_stream_listener_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoStream : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

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

    template <typename TaskT>
    [[nodiscard]] IoStatus read_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return recv_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return recvv_some(task, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return send_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus writev_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return sendv_some(task, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_zc_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return sendv_zc_some(task, iov, iov_count, state);
    }
#endif
};

template <typename ThreadT>
class IoListener : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus bind(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_bind(task, this->thread_, this->fd_, address, address_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus listen(TaskT& task, int backlog) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_listen(task, this->thread_, this->fd_, backlog);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_some(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int* accepted_fd,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_some(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            accepted_fd,
            state,
            flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_direct(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int file_index,
        IoFixedFile<ThreadT>* accepted_file,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_direct(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            flags,
            file_index,
            accepted_file,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_multishot(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int* accepted_fd,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_multishot(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            accepted_fd,
            state,
            flags);
    }
};

