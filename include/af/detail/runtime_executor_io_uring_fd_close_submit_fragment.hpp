#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_fd_close_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_close(
            int fd,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_CLOSE,
                fd,
                nullptr,
                0,
                0,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_shutdown(
            int fd,
            int how,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SHUTDOWN,
                fd,
                nullptr,
                static_cast<std::size_t>(how),
                0,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(how);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
