#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_buffer_register_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_io_uring_buffers(
            const iovec* buffers,
            unsigned buffer_count,
            int* error) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring buffer registration must run on its IO thread");
            if (error != nullptr) {
                *error = 0;
            }
            if (current_thread_index_ != index_ || buffers == nullptr || buffer_count == 0U) {
                if (error != nullptr) {
                    *error = EINVAL;
                }
                return false;
            }

#if defined(__linux__)
            if (io_uring_fd_ < 0) {
                if (error != nullptr) {
                    *error = ENOSYS;
                }
                return false;
            }
            if (io_uring_buffers_registered_) {
                if (error != nullptr) {
                    *error = EALREADY;
                }
                return false;
            }
            if (buffer_count > static_cast<unsigned>(std::numeric_limits<std::uint16_t>::max())) {
                if (error != nullptr) {
                    *error = EINVAL;
                }
                return false;
            }

            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_BUFFERS,
                    buffers,
                    buffer_count) != 0) {
                if (error != nullptr) {
                    *error = errno == 0 ? EIO : errno;
                }
                return false;
            }

            io_uring_buffers_registered_ = true;
            io_uring_registered_buffer_count_ = buffer_count;
            return true;
#else
            if (error != nullptr) {
                *error = ENOSYS;
            }
            return false;
#endif
        }
