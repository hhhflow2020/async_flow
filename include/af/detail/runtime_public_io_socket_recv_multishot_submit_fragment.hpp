#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_socket_recv_multishot_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#if defined(__linux__)
    [[nodiscard]] static bool io_submit_recv_multishot(
        Thread thread,
        int fd,
        std::uint16_t buffer_group,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv_multishot(
            fd,
            buffer_group,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_recvmsg_multishot(
        Thread thread,
        int fd,
        std::uint16_t buffer_group,
        socklen_t name_capacity,
        std::size_t control_capacity,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_multishot(
            fd,
            buffer_group,
            name_capacity,
            control_capacity,
            flags,
            task,
            result);
    }
#endif
