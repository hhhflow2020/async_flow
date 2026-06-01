#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_operation_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void track_io_uring_operation(IoUringOperation* operation) noexcept {
            operation->prev = nullptr;
            operation->next = io_uring_operations_;
            if (io_uring_operations_ != nullptr) {
                io_uring_operations_->prev = operation;
            }
            io_uring_operations_ = operation;
        }

        void untrack_io_uring_operation(IoUringOperation* operation) noexcept {
            if (operation->prev != nullptr) {
                operation->prev->next = operation->next;
            } else if (io_uring_operations_ == operation) {
                io_uring_operations_ = operation->next;
            }
            if (operation->next != nullptr) {
                operation->next->prev = operation->prev;
            }
            operation->prev = nullptr;
            operation->next = nullptr;
        }

        void clear_io_uring_operations() noexcept {
            IoUringOperation* operation = io_uring_operations_;
            io_uring_operations_ = nullptr;
            while (operation != nullptr) {
                IoUringOperation* next = operation->next;
                operation->prev = nullptr;
                operation->next = nullptr;
                close_pending_io_uring_fd_result(operation);
                destroy_io_uring_operation(operation);
                operation = next;
            }
        }

        void fail_io_uring_backend(int error, IoUringOperation* running_operation) noexcept {
            const int backend_error = error == 0 ? EIO : error;
            clear_or_fail_io_uring_operations(error, running_operation);
            close_io_uring_backend();
            io_uring_backend_error_ = backend_error;
        }

        void clear_or_fail_io_uring_operations(
            int error,
            IoUringOperation* running_operation) noexcept {
            IoUringOperation* operation = io_uring_operations_;
            io_uring_operations_ = nullptr;
            while (operation != nullptr) {
                IoUringOperation* next = operation->next;
                operation->prev = nullptr;
                operation->next = nullptr;
                if (operation == running_operation) {
                    close_pending_io_uring_fd_result(operation);
                    destroy_io_uring_operation(operation);
                    operation = next;
                    continue;
                }

                close_pending_io_uring_fd_result(operation);
                if (operation->task == nullptr || operation->result == nullptr) {
                    destroy_io_uring_operation(operation);
                    operation = next;
                    continue;
                }
                operation->result->events = io_error;
                operation->result->error = operation->cancel_requested ? ECANCELED : error;
                operation->result->result = operation->cancel_requested ? -ECANCELED : -error;
                enqueue_pending_blocking(index_, operation->task);
                destroy_io_uring_operation(operation);
                operation = next;
            }
        }

        void close_pending_io_uring_fd_result(IoUringOperation* operation) noexcept {
            if (operation == nullptr ||
                operation->result == nullptr ||
                !io_uring_operation_result_is_fd(operation) ||
                operation->result->error != 0 ||
                (operation->result->events & operation->complete_events) == 0U ||
                operation->result->result < 0) {
                return;
            }
            ::close(static_cast<int>(operation->result->result));
            operation->result->events = io_error;
            operation->result->error = ECANCELED;
            operation->result->result = -ECANCELED;
        }

        static void clear_io_uring_result_token(IoUringOperation* operation) noexcept {
            if (operation != nullptr &&
                operation->result != nullptr &&
                operation->result->completion_token == operation) {
                operation->result->completion_token = nullptr;
            }
        }

        void destroy_io_uring_operation(IoUringOperation* operation) noexcept {
            clear_io_uring_result_token(operation);
            if (operation->msg != nullptr) {
                io_uring_msg_pool_.destroy(operation->msg);
                operation->msg = nullptr;
            }
            if (operation->opcode != IORING_OP_TIMEOUT && operation->socket_address != nullptr) {
                io_uring_address_pool_.destroy(operation->socket_address);
                operation->socket_address = nullptr;
            }
            io_uring_op_pool_.destroy(operation);
        }
#endif
