#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_resource_buffer_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

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
