#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_fd_close_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_close(
        Thread thread,
        int fd,
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
        return executors_[index]->submit_io_uring_close(fd, task, result);
    }

    [[nodiscard]] static bool io_submit_shutdown(
        Thread thread,
        int fd,
        int how,
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
        return executors_[index]->submit_io_uring_shutdown(fd, how, task, result);
    }
