#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_socket_recv_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_recv(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
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

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv(fd, data, size, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_recv_fixed_file(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv_fixed_file(
            file_index,
            data,
            size,
            flags,
            task,
            result);
    }
