#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_filesystem_metadata_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_statx(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mask,
            struct statx* output,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_STATX,
                dir_fd,
                io_readable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.len = mask;
                    sqe.off = reinterpret_cast<std::uint64_t>(output);
                    sqe.statx_flags = static_cast<std::uint32_t>(flags);
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mask);
            static_cast<void>(output);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
