#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_open_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_openat(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mode,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_OPENAT,
                dir_fd,
                const_cast<char*>(path),
                mode,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result);
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mode);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_openat_direct(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mode,
            int file_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_OPENAT,
                dir_fd,
                const_cast<char*>(path),
                mode,
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
                nullptr,
                nullptr,
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
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mode);
            static_cast<void>(file_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_openat2(
            int dir_fd,
            const char* path,
            const struct open_how* how,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_openat2,
                dir_fd,
                io_readable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.len = static_cast<unsigned>(sizeof(*how));
                    sqe.off = reinterpret_cast<std::uint64_t>(how);
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(how);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
