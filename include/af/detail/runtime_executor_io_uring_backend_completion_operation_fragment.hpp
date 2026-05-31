#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_completion_operation_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool complete_io_uring_operation(
            IoUringOperation* operation,
            int result,
            std::uint32_t cqe_flags) noexcept {
            if (operation->poll_wait) {
                complete_io_uring_poll_wait(operation, result);
                return false;
            }

            if (operation->zero_copy_send && (cqe_flags & IORING_CQE_F_NOTIF) != 0U) {
                operation->zero_copy_notification_done = true;
                if (operation->zero_copy_primary_done) {
                    untrack_io_uring_operation(operation);
                    destroy_io_uring_operation(operation);
                }
                return false;
            }

            const bool more =
                operation->multishot &&
                !operation->cancel_requested &&
                result >= 0 &&
                (cqe_flags & IORING_CQE_F_MORE) != 0U;
            const bool zero_copy_waits_for_notification =
                operation->zero_copy_send && (cqe_flags & IORING_CQE_F_MORE) != 0U;
            if (!more && !zero_copy_waits_for_notification) {
                untrack_io_uring_operation(operation);
            }
            operation->result->result = result;
            if (operation->cancel_requested) {
                if (result >= 0) {
                    if (io_uring_operation_result_is_fd(operation)) {
                        ::close(result);
                    } else {
                        clear_direct_io_uring_file_slot(operation);
                    }
                }
                operation->result->events = io_error;
                operation->result->error = ECANCELED;
                operation->result->result = -ECANCELED;
            } else if (result < 0) {
                operation->result->events = io_error;
                operation->result->error = -result;
            } else {
                std::uint32_t events = operation->complete_events | (more ? io_more : 0U);
                if ((cqe_flags & IORING_CQE_F_BUFFER) != 0U) {
                    events |= io_buffer_selected |
                              ((cqe_flags >> io_buffer_id_shift) << io_buffer_id_shift);
                }
                operation->result->events = events;
                operation->result->error = 0;
                if (operation->msg != nullptr && operation->msg->address_size != nullptr) {
                    *operation->msg->address_size = operation->msg->header.msg_namelen;
                }
                if (operation->opcode != IORING_OP_TIMEOUT &&
                    operation->socket_address != nullptr &&
                    operation->socket_address->output_size != nullptr) {
                    const socklen_t actual_size = operation->socket_address->size;
                    if (operation->socket_address->output != nullptr &&
                        operation->socket_address->output_capacity != 0U) {
                        const auto copy_size = static_cast<std::size_t>(
                            std::min(actual_size, operation->socket_address->output_capacity));
                        std::memcpy(
                            operation->socket_address->output,
                            &operation->socket_address->storage,
                            copy_size);
                    }
                    *operation->socket_address->output_size = actual_size;
                }
            }
            enqueue_pending_blocking(index_, operation->task);
            if (more) {
                return true;
            }
            if (zero_copy_waits_for_notification) {
                operation->zero_copy_primary_done = true;
                clear_io_uring_result_token(operation);
                operation->task = nullptr;
                operation->result = nullptr;
                if (operation->zero_copy_notification_done) {
                    untrack_io_uring_operation(operation);
                    destroy_io_uring_operation(operation);
                }
                return true;
            }
            destroy_io_uring_operation(operation);
            return false;
        }
#endif
