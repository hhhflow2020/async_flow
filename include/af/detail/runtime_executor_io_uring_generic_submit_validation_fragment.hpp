#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_validation_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool validate_io_uring_generic_submit(
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind) const noexcept {
            if (current_thread_index_ != index_ || args.task == nullptr || args.result == nullptr) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (io_uring_fd_ < 0 || (!kind.path_fd_op && args.fd < 0)) {
                set_io_uring_generic_submit_error(
                    args.result,
                    args.fd,
                    io_uring_fd_ < 0 ? ENOSYS : EBADF);
                return false;
            }
            if (!kind.data_optional_op && !kind.address_op && args.data == nullptr &&
                !kind.message_iov_op && !args.buffer_select) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (!validate_io_uring_generic_fixed_resources(args, kind)) {
                return false;
            }
            if (args.opcode != IORING_OP_FSYNC && !kind.message_op &&
                args.size > static_cast<std::size_t>(std::numeric_limits<unsigned>::max())) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (kind.message_iov_op &&
                (args.message_iov_count == 0U ||
                 args.message_iov_count > static_cast<std::size_t>(IOV_MAX))) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (kind.connect_op &&
                (args.socket_address == nullptr ||
                 args.socket_address_size == 0U ||
                 args.socket_address_size > static_cast<socklen_t>(sizeof(sockaddr_storage)))) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (kind.accept_op &&
                ((args.socket_address_out == nullptr) != (args.socket_address_size_out == nullptr))) {
                set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                return false;
            }
            if (args.buffer_select &&
                ((args.opcode != IORING_OP_RECV && args.opcode != IORING_OP_RECVMSG) ||
                 !provided_buffer_group_registered(args.provided_buffer_group))) {
                const int error =
                    (args.opcode == IORING_OP_RECV || args.opcode == IORING_OP_RECVMSG) ? ENOBUFS : EINVAL;
                set_io_uring_generic_submit_error(args.result, args.fd, error);
                return false;
            }
            return true;
        }

        [[nodiscard]] bool validate_io_uring_generic_fixed_resources(
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind) const noexcept {
            if (args.fixed_file) {
                if (kind.path_fd_op) {
                    set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                    return false;
                }
                if (!io_uring_files_registered_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
                    return false;
                }
                if (static_cast<unsigned>(args.fd) >= io_uring_registered_file_count_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                    return false;
                }
            }
            if (args.direct_file_index >= 0) {
                if (!(kind.openat_op || kind.accept_op)) {
                    set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                    return false;
                }
                if (!io_uring_files_registered_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
                    return false;
                }
                if (static_cast<unsigned>(args.direct_file_index) >= io_uring_registered_file_count_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                    return false;
                }
            }
            if (kind.fixed_buffer_op) {
                if (!io_uring_buffers_registered_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, ENOBUFS);
                    return false;
                }
                if (args.fixed_buffer_index >= io_uring_registered_buffer_count_) {
                    set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
                    return false;
                }
            }
            return true;
        }
#endif
