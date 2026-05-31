#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_completion_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool poll_io_uring_completions() noexcept {
            if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr || io_uring_cq_tail_ == nullptr) {
                return false;
            }

            bool did_work = false;
            std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
            const std::uint32_t tail = __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
            while (head != tail) {
                io_uring_cqe& cqe = io_uring_cqes_[head & *io_uring_cq_ring_mask_];
                auto* operation = reinterpret_cast<IoUringOperation*>(cqe.user_data);
                if (operation != nullptr) {
                    const bool yield_to_task = complete_io_uring_operation(
                        operation,
                        cqe.res,
                        cqe.flags);
                    did_work = true;
                    ++head;
                    if (yield_to_task) {
                        break;
                    }
                    continue;
                }
                ++head;
            }
            __atomic_store_n(io_uring_cq_head_, head, __ATOMIC_RELEASE);
            return did_work;
        }

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

        void complete_io_uring_poll_wait(
            IoUringOperation* operation,
            int result) noexcept {
            IoWaitRegistration* registration = operation->wait_registration;
            if (registration == nullptr || operation->task == nullptr || operation->result == nullptr) {
                untrack_io_uring_operation(operation);
                destroy_io_uring_operation(operation);
                return;
            }

            const int fd = registration->fd;
            auto it = io_waits_.find(fd);
            if (it != io_waits_.end() && it->second == registration) {
                io_waits_.erase(it);
            }

            registration->result->fd = fd;
            registration->result->result = result;
            if (operation->cancel_requested) {
                registration->result->events = io_error;
                registration->result->error = ECANCELED;
                registration->result->result = -ECANCELED;
            } else if (result < 0) {
                registration->result->events = io_error;
                registration->result->error = -result;
            } else {
                registration->result->events = io_events_from_poll(static_cast<std::uint32_t>(result));
                registration->result->error = 0;
            }

            enqueue_pending_blocking(index_, registration->task);
            registration->poll_operation = nullptr;
            operation->wait_registration = nullptr;
            untrack_io_uring_operation(operation);
            destroy_io_uring_operation(operation);
            io_wait_pool_.destroy(registration);
        }

        [[nodiscard]] static bool io_uring_result_is_fd(std::uint8_t opcode) noexcept {
            return opcode == IORING_OP_OPENAT ||
                   opcode == IORING_OP_ACCEPT ||
                   opcode == detail::io_uring_op_openat2 ||
                   opcode == detail::io_uring_op_socket;
        }

        [[nodiscard]] static bool io_uring_operation_result_is_fd(
            const IoUringOperation* operation) noexcept {
            return operation != nullptr &&
                   operation->direct_file_index < 0 &&
                   io_uring_result_is_fd(operation->opcode);
        }

        void clear_direct_io_uring_file_slot(const IoUringOperation* operation) noexcept {
            if (operation == nullptr ||
                operation->direct_file_index < 0 ||
                !io_uring_result_is_fd(operation->opcode) ||
                io_uring_fd_ < 0 ||
                !io_uring_files_registered_) {
                return;
            }
            const int invalid_fd = -1;
            io_uring_files_update update{};
            update.offset = static_cast<unsigned>(operation->direct_file_index);
            update.fds = reinterpret_cast<std::uint64_t>(&invalid_fd);
            static_cast<void>(detail::sys_io_uring_register(
                io_uring_fd_,
                IORING_REGISTER_FILES_UPDATE,
                &update,
                1));
        }

        [[nodiscard]] int submit_io_uring_cancel(IoUringOperation* operation) noexcept {
            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                return reserve_error == 0 ? EBUSY : reserve_error;
            }

            *sqe = io_uring_sqe{};
            sqe->opcode = IORING_OP_ASYNC_CANCEL;
            sqe->fd = -1;
            sqe->addr = reinterpret_cast<std::uint64_t>(operation);
            sqe->cancel_flags = 0;
            sqe->user_data = 0;

            const int submit_error = flush_io_uring_submissions();
            if (submit_error != 0) {
                fail_io_uring_backend(submit_error, nullptr);
            }
            return submit_error;
        }
#endif
