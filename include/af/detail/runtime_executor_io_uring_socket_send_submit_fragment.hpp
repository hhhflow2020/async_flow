#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_send_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_send(
            int fd,
            const void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_buffer_op(
                IORING_OP_SEND,
                fd,
                const_cast<void*>(data),
                size,
                0,
                flags,
                io_writable,
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

        [[nodiscard]] bool submit_io_uring_send_fixed_file(
            int file_index,
            const void* data,
            std::size_t size,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SEND,
                file_index,
                const_cast<void*>(data),
                size,
                0,
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
