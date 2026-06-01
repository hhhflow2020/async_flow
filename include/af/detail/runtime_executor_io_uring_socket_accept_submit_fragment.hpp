#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_accept_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if !defined(_WIN32)
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
#endif
