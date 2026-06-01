#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_connect_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if !defined(_WIN32)
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
