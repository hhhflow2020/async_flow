#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_recv(
            int fd,
            void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_buffer_op(
                IORING_OP_RECV,
                fd,
                data,
                size,
                0,
                flags,
                io_readable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_recv_fixed_file(
            int file_index,
            void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_RECV,
                file_index,
                data,
                size,
                0,
                flags,
                io_readable,
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
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

#if defined(__linux__)
        [[nodiscard]] bool submit_io_uring_recv_multishot(
            int fd,
            std::uint16_t buffer_group,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!provided_buffer_group_registered(buffer_group)) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOBUFS;
                }
                return false;
            }
            return submit_io_uring_op(
                IORING_OP_RECV,
                fd,
                nullptr,
                0,
                0,
                flags,
                io_readable,
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
                true,
                false,
                buffer_group,
                true);
        }

        [[nodiscard]] bool submit_io_uring_recvmsg_multishot(
            int fd,
            std::uint16_t buffer_group,
            socklen_t name_capacity,
            std::size_t control_capacity,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!provided_buffer_group_registered(buffer_group)) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOBUFS;
                }
                return false;
            }
            return submit_io_uring_op(
                IORING_OP_RECVMSG,
                fd,
                nullptr,
                control_capacity,
                0,
                flags,
                io_readable,
                task,
                result,
                nullptr,
                name_capacity,
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
                true,
                false,
                buffer_group,
                true);
        }
#endif

        [[nodiscard]] bool submit_io_uring_send(
            int fd,
            const void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_buffer_op(
                IORING_OP_SEND,
                fd,
                const_cast<void*>(data),
                size,
                0,
                flags,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_send_fixed_file(
            int file_index,
            const void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SEND,
                file_index,
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
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

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

#if !defined(_WIN32)
        [[nodiscard]] bool submit_io_uring_recvmsg_fixed_file_iov(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_RECVMSG,
                file_index,
                nullptr,
                0,
                0,
                flags,
                io_readable,
                task,
                result,
                nullptr,
                0,
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
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_recvmsg_iov(
            int fd,
            const iovec* iov,
            int iov_count,
            sockaddr* address,
            socklen_t* address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_RECVMSG,
                fd,
                nullptr,
                0,
                0,
                flags,
                io_readable,
                task,
                result,
                address,
                address_size == nullptr ? 0 : *address_size,
                address_size,
                nullptr,
                0,
                nullptr,
                nullptr,
                iov,
                static_cast<std::size_t>(iov_count));
#else
            static_cast<void>(fd);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_sendmsg_fixed_file_iov(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SENDMSG,
                file_index,
                nullptr,
                0,
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
                iov,
                static_cast<std::size_t>(iov_count),
                0,
                -1,
                0,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_sendmsg_iov(
            int fd,
            const iovec* iov,
            int iov_count,
            const sockaddr* address,
            socklen_t address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SENDMSG,
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
                static_cast<std::size_t>(iov_count));
#else
            static_cast<void>(fd);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_recvmsg(
            int fd,
            void* data,
            std::size_t size,
            sockaddr* address,
            socklen_t* address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_RECVMSG,
                fd,
                data,
                size,
                0,
                flags,
                io_readable,
                task,
                result,
                address,
                address_size == nullptr ? 0 : *address_size,
                address_size);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_sendmsg(
            int fd,
            const void* data,
            std::size_t size,
            const sockaddr* address,
            socklen_t address_size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SENDMSG,
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
                nullptr);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_accept(
            int fd,
            sockaddr* address,
            socklen_t* address_size,
            int flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_ACCEPT,
                fd,
                nullptr,
                0,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                address,
                address_size);
#else
            static_cast<void>(fd);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_accept_direct(
            int fd,
            sockaddr* address,
            socklen_t* address_size,
            int flags,
            int file_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_ACCEPT,
                fd,
                nullptr,
                0,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                address,
                address_size,
                nullptr,
                0,
                0,
                -1,
                0,
                false,
                false,
                false,
                0,
                false,
                file_index);
#else
            static_cast<void>(fd);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(file_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_accept_multishot(
            int fd,
            sockaddr* address,
            socklen_t* address_size,
            int flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_ACCEPT,
                fd,
                nullptr,
                0,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                address,
                address_size,
                nullptr,
                0,
                0,
                -1,
                0,
                false,
                true);
#else
            static_cast<void>(fd);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_connect(
            int fd,
            const sockaddr* address,
            socklen_t address_size,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_CONNECT,
                fd,
                nullptr,
                0,
                0,
                0,
                io_writable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                address,
                address_size,
                nullptr,
                nullptr);
#else
            static_cast<void>(fd);
            static_cast<void>(address);
            static_cast<void>(address_size);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
#endif

        [[nodiscard]] bool submit_io_uring_socket(
            int domain,
            int type,
            int protocol,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            return submit_io_uring_socket_impl(domain, type, protocol, flags, task, result);
        }

