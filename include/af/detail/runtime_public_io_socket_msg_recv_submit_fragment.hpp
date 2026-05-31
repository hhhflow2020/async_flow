#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_socket_msg_recv_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_recvmsg_fixed_file_iov(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_fixed_file_iov(
            file_index,
            iov,
            iov_count,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(flags);
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_recvmsg_iov(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        sockaddr* address,
        socklen_t* address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_iov(
            fd,
            iov,
            iov_count,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_recvmsg(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        sockaddr* address,
        socklen_t* address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg(
            fd,
            data,
            size,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(size);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }
#endif
