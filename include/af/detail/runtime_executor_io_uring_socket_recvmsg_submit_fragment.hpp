#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_recvmsg_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
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

#endif
