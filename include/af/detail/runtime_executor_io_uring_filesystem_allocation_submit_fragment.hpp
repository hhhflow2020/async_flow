#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_filesystem_allocation_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_fallocate(
            int fd,
            int mode,
            std::uint64_t offset,
            std::uint64_t length,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_FALLOCATE,
                fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = fd;
                    sqe.addr = length;
                    sqe.len = static_cast<unsigned>(mode);
                    sqe.off = offset;
                });
#else
            static_cast<void>(fd);
            static_cast<void>(mode);
            static_cast<void>(offset);
            static_cast<void>(length);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_ftruncate(
            int fd,
            std::uint64_t length,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_ftruncate,
                fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = fd;
                    sqe.off = length;
                });
#else
            static_cast<void>(fd);
            static_cast<void>(length);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
