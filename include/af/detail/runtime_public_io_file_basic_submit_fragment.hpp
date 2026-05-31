#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_file_basic_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_read_at(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        std::uint64_t offset,
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
        return executors_[index]->submit_io_uring_read(fd, data, size, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_write_at(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
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
        return executors_[index]->submit_io_uring_write(fd, data, size, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_fsync(
        Thread thread,
        int fd,
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
        return executors_[index]->submit_io_uring_fsync(fd, flags, task, result);
    }
