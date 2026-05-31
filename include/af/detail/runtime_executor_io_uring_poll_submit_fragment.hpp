#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_poll_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool provided_buffer_group_registered(
            std::uint16_t buffer_group) const noexcept {
            return std::find(
                io_uring_provided_buffer_groups_.begin(),
                io_uring_provided_buffer_groups_.end(),
                buffer_group) != io_uring_provided_buffer_groups_.end();
        }

        [[nodiscard]] IoUringPollSubmitResult try_submit_io_uring_poll_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            IoWaitRegistration* registration) noexcept {
            if (!io_uring_thread() || io_uring_fd_ < 0 || !io_uring_poll_add_available_) {
                return IoUringPollSubmitResult::Fallback;
            }

            const std::uint32_t native_events = native_poll_events(events);
            if (native_events == 0U) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return IoUringPollSubmitResult::Failed;
            }

            IoUringOperation* operation = nullptr;
            try {
                operation = io_uring_op_pool_.create();
            } catch (...) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOMEM;
                return IoUringPollSubmitResult::Failed;
            }

            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                io_uring_op_pool_.destroy(operation);
                result->fd = fd;
                result->events = io_error;
                result->error = reserve_error == 0 ? EBUSY : reserve_error;
                if (io_uring_fd_ < 0) {
                    return IoUringPollSubmitResult::BackendClosed;
                }
                return IoUringPollSubmitResult::Fallback;
            }

            operation->task = task;
            operation->result = result;
            operation->prev = nullptr;
            operation->next = nullptr;
            operation->msg = nullptr;
            operation->socket_address = nullptr;
            operation->wait_registration = registration;
            operation->complete_events = 0;
            operation->direct_file_index = -1;
            operation->opcode = IORING_OP_POLL_ADD;
            operation->cancel_requested = false;
            operation->multishot = false;
            operation->poll_wait = true;
            operation->zero_copy_send = false;
            operation->zero_copy_primary_done = false;
            operation->zero_copy_notification_done = false;
            registration->poll_operation = operation;

            track_io_uring_operation(operation);

            *sqe = io_uring_sqe{};
            sqe->opcode = IORING_OP_POLL_ADD;
            sqe->fd = fd;
            sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
            sqe->poll32_events = native_events;

            result->fd = fd;
            result->events = 0;
            result->error = 0;
            result->result = 0;
            result->completion_token = nullptr;

            if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
                const int submit_error = flush_io_uring_submissions();
                if (submit_error == 0) {
                    return IoUringPollSubmitResult::Submitted;
                }
                result->events = io_error;
                result->error = submit_error;
                result->result = -submit_error;
                fail_io_uring_backend(submit_error, operation);
                return IoUringPollSubmitResult::BackendClosed;
            }

            return IoUringPollSubmitResult::Submitted;
        }

#endif
