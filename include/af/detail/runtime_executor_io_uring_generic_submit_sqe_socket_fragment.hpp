#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_sqe_socket_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        static void fill_io_uring_generic_message_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args,
            IoUringOperation* operation) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(&operation->msg->header);
            sqe.len = 1U;
            sqe.msg_flags = args.op_flags;
            if (args.opcode == IORING_OP_RECVMSG && args.multishot) {
                sqe.ioprio |= IORING_RECV_MULTISHOT;
            }
        }

        static void fill_io_uring_generic_accept_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args,
            IoUringOperation* operation) noexcept {
            if (operation->socket_address != nullptr) {
                sqe.addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
                sqe.addr2 = reinterpret_cast<std::uint64_t>(&operation->socket_address->size);
            }
            sqe.accept_flags = args.op_flags;
            if (args.multishot) {
                sqe.ioprio |= IORING_ACCEPT_MULTISHOT;
            }
        }

        static void fill_io_uring_generic_connect_sqe(
            io_uring_sqe& sqe,
            IoUringOperation* operation) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
            sqe.off = operation->socket_address->size;
        }

        static void fill_io_uring_generic_socket_data_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.msg_flags = args.op_flags;
            if (args.opcode == IORING_OP_RECV && args.multishot) {
                sqe.ioprio |= IORING_RECV_MULTISHOT;
            }
        }
#endif
