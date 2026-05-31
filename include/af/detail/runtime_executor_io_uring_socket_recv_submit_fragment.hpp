#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_recv_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
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
