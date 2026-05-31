#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_file_fixed_buffer_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_read_fixed_file_at(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
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
        return executors_[index]->submit_io_uring_read_fixed_file(
            file_index,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(
        Thread thread,
        int file_index,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
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
        return executors_[index]->submit_io_uring_write_fixed_file(
            file_index,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_at(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
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
        return executors_[index]->submit_io_uring_read_fixed(
            fd,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_at(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
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
        return executors_[index]->submit_io_uring_write_fixed(
            fd,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }
#endif
