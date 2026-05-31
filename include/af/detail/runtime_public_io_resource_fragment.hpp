#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_resource_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool io_backend_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_backend_available();
    }

    [[nodiscard]] static bool io_uring_backend_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_uring_backend_available();
    }

    [[nodiscard]] static bool io_uring_poll_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_uring_poll_available();
    }

#if !defined(_WIN32)
    [[nodiscard]] static bool io_register_buffers(
        Thread thread,
        const iovec* buffers,
        unsigned buffer_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (buffers == nullptr || buffer_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_buffers(buffers, buffer_count, error);
    }

    [[nodiscard]] static bool io_unregister_buffers(
        Thread thread,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_buffers(error);
    }
#endif

    [[nodiscard]] static bool io_register_provided_buffer_ring(
        Thread thread,
        void* ring,
        unsigned ring_entries,
        std::uint16_t buffer_group,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (ring == nullptr || ring_entries == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_provided_buffer_ring(
            ring,
            ring_entries,
            buffer_group,
            error);
    }

    [[nodiscard]] static bool io_unregister_provided_buffer_ring(
        Thread thread,
        std::uint16_t buffer_group,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_provided_buffer_ring(
            buffer_group,
            error);
    }

    [[nodiscard]] static bool io_register_files(
        Thread thread,
        const int* files,
        unsigned file_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_files(files, file_count, error);
    }

    [[nodiscard]] static bool io_update_registered_files(
        Thread thread,
        unsigned offset,
        const int* files,
        unsigned file_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->update_io_uring_files(offset, files, file_count, error);
    }

    [[nodiscard]] static bool io_unregister_files(
        Thread thread,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_files(error);
    }

    [[nodiscard]] static bool io_wait(
        Thread thread,
        int fd,
        std::uint32_t events,
        Task* task,
        IoResult* result,
        bool prefer_rearm = false) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || events == 0U) {
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
        return executors_[index]->register_io_wait(fd, events, task, result, prefer_rearm);
    }

    [[nodiscard]] static bool cancel_io(Thread thread, IoOpState& state) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            state.wait.fd = -1;
            state.wait.events = io_error;
            state.wait.error = EINVAL;
            state.wait.result = -EINVAL;
            return false;
        }
        return executors_[index]->cancel_io(state);
    }

    [[nodiscard]] static bool io_submit_timeout(
        Thread thread,
        std::chrono::nanoseconds timeout,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || timeout.count() <= 0) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
                result->result = -EINVAL;
                result->completion_token = nullptr;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = -1;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_timeout(timeout, task, result);
    }
