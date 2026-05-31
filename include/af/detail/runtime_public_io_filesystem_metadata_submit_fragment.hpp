#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_filesystem_metadata_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_statx(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        std::uint32_t mask,
        struct statx* output,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || output == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_statx(
            dir_fd,
            path,
            flags,
            mask,
            output,
            task,
            result);
    }
