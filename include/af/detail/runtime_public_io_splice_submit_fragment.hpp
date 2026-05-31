#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_splice_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_splice(
        Thread thread,
        int in_fd,
        std::int64_t off_in,
        int out_fd,
        std::int64_t off_out,
        std::size_t count,
        unsigned int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || in_fd < 0 || out_fd < 0) {
            if (result != nullptr) {
                result->fd = out_fd;
                result->events = io_error;
                result->error = in_fd < 0 || out_fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = out_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_splice(
            in_fd,
            off_in,
            out_fd,
            off_out,
            count,
            flags,
            task,
            result);
    }
