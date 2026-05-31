#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_file_submit_fragment.hpp is an AsyncRuntime implementation fragment"
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

    [[nodiscard]] static bool io_submit_read_fixed_file_at(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint64_t offset,
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
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(
        Thread thread,
        int file_index,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
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
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_fsync_fixed_file(
        Thread thread,
        int file_index,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0) {
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
        return executors_[index]->submit_io_uring_fsync_fixed_file(
            file_index,
            flags,
            task,
            result);
    }

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_readv_fixed_file_at(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
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

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_readv_fixed_file(
            file_index,
            iov,
            iov_count,
            offset,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_writev_fixed_file_at(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
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

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_writev_fixed_file(
            file_index,
            iov,
            iov_count,
            offset,
            task,
            result);
    }

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

    [[nodiscard]] static bool io_submit_readv_at(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
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

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_readv(fd, iov, iov_count, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_writev_at(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
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

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_writev(fd, iov, iov_count, offset, task, result);
    }
#endif

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

    [[nodiscard]] static bool io_submit_openat(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
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
        return executors_[index]->submit_io_uring_openat(
            dir_fd,
            path,
            flags,
            mode,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_socket(
        Thread thread,
        int domain,
        int type,
        int protocol,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = -1;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_socket(
            domain,
            type,
            protocol,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_openat_direct(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        std::uint32_t mode,
        int file_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || file_index < 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
                result->result = -result->error;
                result->completion_token = nullptr;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_uring_openat_direct(
            dir_fd,
            path,
            flags,
            mode,
            file_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_openat2(
        Thread thread,
        int dir_fd,
        const char* path,
        const struct open_how* how,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || how == nullptr) {
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
        return executors_[index]->submit_io_uring_openat2(
            dir_fd,
            path,
            how,
            task,
            result);
    }

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

    [[nodiscard]] static bool io_submit_fallocate(
        Thread thread,
        int fd,
        int mode,
        std::uint64_t offset,
        std::uint64_t length,
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
        return executors_[index]->submit_io_uring_fallocate(
            fd,
            mode,
            offset,
            length,
            task,
            result);
    }

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

    [[nodiscard]] static bool io_submit_ftruncate(
        Thread thread,
        int fd,
        std::uint64_t length,
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
        return executors_[index]->submit_io_uring_ftruncate(fd, length, task, result);
    }

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

