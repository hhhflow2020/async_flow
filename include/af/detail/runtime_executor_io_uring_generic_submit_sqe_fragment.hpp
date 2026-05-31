#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_sqe_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        static void fill_io_uring_generic_submit_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind,
            IoUringOperation* operation) noexcept {
            sqe = io_uring_sqe{};
            sqe.opcode = args.opcode;
            sqe.fd = args.fd;
            sqe.user_data = reinterpret_cast<std::uint64_t>(operation);
            if (args.fixed_file) {
                sqe.flags |= IOSQE_FIXED_FILE;
            }
            if (args.buffer_select) {
                sqe.flags |= IOSQE_BUFFER_SELECT;
                sqe.buf_index = args.provided_buffer_group;
            }
            if (args.direct_file_index >= 0) {
                sqe.file_index = static_cast<std::uint32_t>(args.direct_file_index) + 1U;
            }

            if (args.opcode == IORING_OP_FSYNC) {
                sqe.fsync_flags = args.op_flags;
            } else if (kind.close_op) {
                // fd is already filled.
            } else if (kind.shutdown_op) {
                sqe.len = static_cast<unsigned>(args.size);
            } else if (kind.fallocate_op) {
                fill_io_uring_generic_fallocate_sqe(sqe, args);
            } else if (kind.splice_op) {
                fill_io_uring_generic_splice_sqe(sqe, args);
            } else if (kind.openat_op) {
                fill_io_uring_generic_openat_sqe(sqe, args);
            } else if (kind.statx_op) {
                fill_io_uring_generic_statx_sqe(sqe, args);
            } else if (kind.renameat_op) {
                fill_io_uring_generic_renameat_sqe(sqe, args);
            } else if (kind.unlinkat_op) {
                fill_io_uring_generic_unlinkat_sqe(sqe, args);
            } else if (kind.message_op) {
                fill_io_uring_generic_message_sqe(sqe, args, operation);
            } else if (kind.accept_op) {
                fill_io_uring_generic_accept_sqe(sqe, args, operation);
            } else if (kind.connect_op) {
                fill_io_uring_generic_connect_sqe(sqe, operation);
            } else if (kind.fixed_buffer_op) {
                fill_io_uring_generic_fixed_buffer_sqe(sqe, args);
            } else if (args.opcode == IORING_OP_RECV ||
                       args.opcode == IORING_OP_SEND ||
                       args.opcode == detail::io_uring_op_send_zc) {
                fill_io_uring_generic_socket_data_sqe(sqe, args);
            } else {
                fill_io_uring_generic_buffer_sqe(sqe, args);
            }
        }

        static void fill_io_uring_generic_fallocate_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = args.extra;
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
        }

        static void fill_io_uring_generic_splice_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = args.extra;
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.splice_fd_in = args.extra_fd;
            sqe.splice_flags = args.op_flags;
        }

        static void fill_io_uring_generic_openat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.open_flags = args.op_flags;
        }

        static void fill_io_uring_generic_statx_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.statx_flags = args.op_flags;
        }

        static void fill_io_uring_generic_renameat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.rename_flags = args.op_flags;
        }

        static void fill_io_uring_generic_unlinkat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.unlink_flags = args.op_flags;
        }

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

        static void fill_io_uring_generic_fixed_buffer_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.buf_index = args.fixed_buffer_index;
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

        static void fill_io_uring_generic_buffer_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
        }
#endif
