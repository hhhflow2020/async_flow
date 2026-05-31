#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_splice_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_splice(
            int in_fd,
            std::int64_t off_in,
            int out_fd,
            std::int64_t off_out,
            std::size_t count,
            unsigned int flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SPLICE,
                out_fd,
                nullptr,
                count,
                static_cast<std::uint64_t>(off_out),
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
                static_cast<std::uint64_t>(off_in),
                in_fd);
#else
            static_cast<void>(in_fd);
            static_cast<void>(off_in);
            static_cast<void>(out_fd);
            static_cast<void>(off_out);
            static_cast<void>(count);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
