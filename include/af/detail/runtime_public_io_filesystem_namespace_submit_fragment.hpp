#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_filesystem_namespace_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_submit_renameat(
        Thread thread,
        int old_dir_fd,
        const char* old_path,
        int new_dir_fd,
        const char* new_path,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            if (result != nullptr) {
                result->fd = old_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = old_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_renameat(
            old_dir_fd,
            old_path,
            new_dir_fd,
            new_path,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_unlinkat(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
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
        return executors_[index]->submit_io_uring_unlinkat(
            dir_fd,
            path,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_mkdirat(
        Thread thread,
        int dir_fd,
        const char* path,
        std::uint32_t mode,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
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
        return executors_[index]->submit_io_uring_mkdirat(dir_fd, path, mode, task, result);
    }

    [[nodiscard]] static bool io_submit_symlinkat(
        Thread thread,
        const char* target,
        int new_dir_fd,
        const char* link_path,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || target == nullptr || link_path == nullptr) {
            if (result != nullptr) {
                result->fd = new_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = new_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_symlinkat(
            target,
            new_dir_fd,
            link_path,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_linkat(
        Thread thread,
        int old_dir_fd,
        const char* old_path,
        int new_dir_fd,
        const char* new_path,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            if (result != nullptr) {
                result->fd = old_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = old_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_linkat(
            old_dir_fd,
            old_path,
            new_dir_fd,
            new_path,
            flags,
            task,
            result);
    }
