#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_timeout_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_timeout(
            std::chrono::nanoseconds timeout,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_timeout(timeout, task, result);
#elif AF_DETAIL_HAS_KQUEUE
            return submit_kqueue_timeout(timeout, task, result);
#else
            static_cast<void>(timeout);
            static_cast<void>(task);
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOSYS;
                result->result = -ENOSYS;
                result->completion_token = nullptr;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_timeout(
            std::chrono::nanoseconds timeout,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            AF_ASSERT(current_thread_index_ == index_ && "io_uring timeout submit must be called from its IO thread");
            if (result != nullptr) {
                result->completion_token = nullptr;
            }
            if (current_thread_index_ != index_ ||
                task == nullptr ||
                result == nullptr ||
                timeout.count() <= 0) {
                if (result != nullptr) {
                    result->fd = -1;
                    result->events = io_error;
                    result->error = EINVAL;
                    result->result = -EINVAL;
                }
                return false;
            }
            if (io_uring_fd_ < 0) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOSYS;
                result->result = -ENOSYS;
                return false;
            }

            IoUringOperation* operation = nullptr;
            try {
                operation = io_uring_op_pool_.create();
            } catch (...) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOMEM;
                result->result = -ENOMEM;
                return false;
            }

            const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
            const auto nanoseconds = timeout - seconds;
            operation->task = task;
            operation->result = result;
            operation->prev = nullptr;
            operation->next = nullptr;
            operation->msg = nullptr;
            operation->socket_address = nullptr;
            operation->wait_registration = nullptr;
            operation->timeout.tv_sec = seconds.count();
            operation->timeout.tv_nsec = nanoseconds.count();
            operation->complete_events = io_readable;
            operation->direct_file_index = -1;
            operation->opcode = IORING_OP_TIMEOUT;
            operation->cancel_requested = false;
            operation->multishot = false;
            operation->poll_wait = false;
            operation->zero_copy_send = false;
            operation->zero_copy_primary_done = false;
            operation->zero_copy_notification_done = false;

            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                destroy_io_uring_operation(operation);
                result->fd = -1;
                result->events = io_error;
                result->error = reserve_error == 0 ? EBUSY : reserve_error;
                result->result = -result->error;
                return false;
            }

            track_io_uring_operation(operation);

            *sqe = io_uring_sqe{};
            sqe->opcode = IORING_OP_TIMEOUT;
            sqe->fd = -1;
            sqe->addr = reinterpret_cast<std::uint64_t>(&operation->timeout);
            sqe->len = 1U;
            sqe->off = 0U;
            sqe->timeout_flags = 0U;
            sqe->user_data = reinterpret_cast<std::uint64_t>(operation);

            result->fd = -1;
            result->events = 0;
            result->error = 0;
            result->result = 0;
            result->completion_token = operation;

            if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
                const int submit_error = flush_io_uring_submissions();
                if (submit_error == 0) {
                    return true;
                }
                result->events = io_error;
                result->error = submit_error;
                result->result = -submit_error;
                fail_io_uring_backend(submit_error, operation);
                return false;
            }
            return true;
#else
            static_cast<void>(timeout);
            static_cast<void>(task);
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOSYS;
                result->result = -ENOSYS;
                result->completion_token = nullptr;
            }
            return false;
#endif
        }
