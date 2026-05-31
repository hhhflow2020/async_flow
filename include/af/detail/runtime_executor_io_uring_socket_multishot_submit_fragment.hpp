#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_multishot_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool submit_io_uring_recv_multishot(
            int fd,
            std::uint16_t buffer_group,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!provided_buffer_group_registered(buffer_group)) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOBUFS;
                }
                return false;
            }
            return submit_io_uring_op(
                IORING_OP_RECV,
                fd,
                nullptr,
                0,
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
                false,
                true,
                false,
                buffer_group,
                true);
        }

        [[nodiscard]] bool submit_io_uring_recvmsg_multishot(
            int fd,
            std::uint16_t buffer_group,
            socklen_t name_capacity,
            std::size_t control_capacity,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            if (!provided_buffer_group_registered(buffer_group)) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOBUFS;
                }
                return false;
            }
            return submit_io_uring_op(
                IORING_OP_RECVMSG,
                fd,
                nullptr,
                control_capacity,
                0,
                flags,
                io_readable,
                task,
                result,
                nullptr,
                name_capacity,
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
                true,
                false,
                buffer_group,
                true);
        }
#endif
