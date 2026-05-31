#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_operation_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] IoUringOperation* create_io_uring_generic_submit_operation(
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind) noexcept {
            IoUringOperation* operation = nullptr;
            try {
                operation = io_uring_op_pool_.create();
            } catch (...) {
                set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
                return nullptr;
            }

            initialize_io_uring_generic_submit_operation(args, operation);

            if (kind.message_op && !attach_io_uring_generic_submit_message(args, kind, operation)) {
                return nullptr;
            }
            if (kind.needs_socket_address &&
                !attach_io_uring_generic_submit_socket_address(args, kind, operation)) {
                return nullptr;
            }
            return operation;
        }

        static void initialize_io_uring_generic_submit_operation(
            const IoUringGenericSubmitArgs& args,
            IoUringOperation* operation) noexcept {
            operation->task = args.task;
            operation->result = args.result;
            operation->complete_events = args.complete_events;
            operation->direct_file_index = args.direct_file_index;
            operation->opcode = args.opcode;
            operation->cancel_requested = false;
            operation->multishot = args.multishot;
            operation->poll_wait = false;
            operation->zero_copy_send = args.zero_copy_send;
            operation->zero_copy_primary_done = false;
            operation->zero_copy_notification_done = false;
            operation->msg = nullptr;
            operation->socket_address = nullptr;
            operation->wait_registration = nullptr;
        }

        [[nodiscard]] bool attach_io_uring_generic_submit_message(
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind,
            IoUringOperation* operation) noexcept {
            try {
                operation->msg = io_uring_msg_pool_.create();
            } catch (...) {
                io_uring_op_pool_.destroy(operation);
                set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
                return false;
            }

            operation->msg->header = msghdr{};
            operation->msg->header.msg_name = args.message_name;
            operation->msg->header.msg_namelen = args.message_name_len;
            if (args.opcode == IORING_OP_RECVMSG && args.multishot && args.buffer_select) {
                operation->msg->header.msg_controllen = args.size;
            } else if (kind.message_iov_op) {
                operation->msg->header.msg_iov = const_cast<iovec*>(args.message_iov);
                operation->msg->header.msg_iovlen = args.message_iov_count;
            } else {
                operation->msg->iov = iovec{args.data, args.size};
                operation->msg->header.msg_iov = &operation->msg->iov;
                operation->msg->header.msg_iovlen = 1;
            }
            operation->msg->address_size = args.message_name_len_out;
            return true;
        }

        [[nodiscard]] bool attach_io_uring_generic_submit_socket_address(
            const IoUringGenericSubmitArgs& args,
            const IoUringGenericSubmitKind& kind,
            IoUringOperation* operation) noexcept {
            try {
                operation->socket_address = io_uring_address_pool_.create();
            } catch (...) {
                destroy_io_uring_operation(operation);
                set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
                return false;
            }

            operation->socket_address->storage = sockaddr_storage{};
            operation->socket_address->output = nullptr;
            operation->socket_address->output_size = nullptr;
            operation->socket_address->output_capacity = 0;
            if (kind.connect_op) {
                std::memcpy(
                    &operation->socket_address->storage,
                    args.socket_address,
                    args.socket_address_size);
                operation->socket_address->size = args.socket_address_size;
            } else {
                operation->socket_address->size = sizeof(operation->socket_address->storage);
                operation->socket_address->output = args.socket_address_out;
                operation->socket_address->output_size = args.socket_address_size_out;
                operation->socket_address->output_capacity =
                    args.socket_address_size_out == nullptr ? 0 : *args.socket_address_size_out;
            }
            return true;
        }
#endif
