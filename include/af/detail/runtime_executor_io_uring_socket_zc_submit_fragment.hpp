#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_zc_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool submit_io_uring_send_zc(
            int fd,
            const void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!io_uring_send_zc_available_) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOSYS;
                }
                return false;
            }
            return submit_io_uring_op(
                detail::io_uring_op_send_zc,
                fd,
                const_cast<void*>(data),
                size,
                0,
                flags,
                io_writable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                0,
                false,
                false,
                true);
        }

        [[nodiscard]] bool submit_io_uring_sendmsg_zc(
            int fd,
            const void* data,
            std::size_t size,
            const sockaddr* address,
            socklen_t address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!io_uring_sendmsg_zc_available_) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOSYS;
                }
                return false;
            }
            return submit_io_uring_op(
                detail::io_uring_op_sendmsg_zc,
                fd,
                const_cast<void*>(data),
                size,
                0,
                flags,
                io_writable,
                task,
                result,
                const_cast<sockaddr*>(address),
                address_size,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                0,
                false,
                false,
                true);
        }

        [[nodiscard]] bool submit_io_uring_sendmsg_zc_iov(
            int fd,
            const iovec* iov,
            int iov_count,
            const sockaddr* address,
            socklen_t address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!io_uring_sendmsg_zc_available_) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOSYS;
                }
                return false;
            }
            return submit_io_uring_op(
                detail::io_uring_op_sendmsg_zc,
                fd,
                nullptr,
                0,
                0,
                flags,
                io_writable,
                task,
                result,
                const_cast<sockaddr*>(address),
                address_size,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                iov,
                static_cast<std::size_t>(iov_count),
                0,
                -1,
                0,
                false,
                false,
                true);
        }
#endif
