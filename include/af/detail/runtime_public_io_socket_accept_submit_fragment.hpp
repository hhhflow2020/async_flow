#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_socket_accept_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_accept(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
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
        return executors_[index]->submit_io_uring_accept(
            fd,
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

    [[nodiscard]] static bool io_submit_accept_direct(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        int file_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || file_index < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = fd < 0 || file_index < 0 ? EBADF : EINVAL;
                result->result = -result->error;
                result->completion_token = nullptr;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_uring_accept_direct(
            fd,
            address,
            address_size,
            flags,
            file_index,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOSYS;
        result->result = -ENOSYS;
        result->completion_token = nullptr;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_accept_multishot(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 ||
            address != nullptr || address_size != nullptr) {
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
        return executors_[index]->submit_io_uring_accept_multishot(
            fd,
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
#endif
