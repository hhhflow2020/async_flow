#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_fixed_vectored_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if !defined(_WIN32)
        [[nodiscard]] bool submit_io_uring_readv_fixed_file(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_READV,
                file_index,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                io_readable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_writev_fixed_file(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_WRITEV,
                file_index,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                io_writable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

#endif
