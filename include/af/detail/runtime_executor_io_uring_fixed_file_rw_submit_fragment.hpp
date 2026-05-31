#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_fixed_file_rw_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool submit_io_uring_fixed_file_rw(
            std::uint8_t opcode,
            int file_index,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint32_t complete_events,
            Task* task,
            IoResult* result,
            std::uint16_t fixed_buffer_index = 0,
            bool fixed_buffer = false) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring submit must be called from its IO thread");
            if (result != nullptr) {
                result->completion_token = nullptr;
            }
            if (current_thread_index_ != index_ || task == nullptr || result == nullptr) {
                if (result != nullptr) {
                    result->fd = file_index;
                    result->events = io_error;
                    result->error = EINVAL;
                }
                return false;
            }
            if (io_uring_fd_ < 0 || file_index < 0) {
                result->fd = file_index;
                result->events = io_error;
                result->error = io_uring_fd_ < 0 ? ENOSYS : EBADF;
                return false;
            }
            if (data == nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (!io_uring_files_registered_) {
                result->fd = file_index;
                result->events = io_error;
                result->error = ENXIO;
                return false;
            }
            if (static_cast<unsigned>(file_index) >= io_uring_registered_file_count_) {
                result->fd = file_index;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (fixed_buffer) {
                if (!io_uring_buffers_registered_) {
                    result->fd = file_index;
                    result->events = io_error;
                    result->error = ENOBUFS;
                    return false;
                }
                if (fixed_buffer_index >= io_uring_registered_buffer_count_) {
                    result->fd = file_index;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
            }
            if (!detail::io_uring_sqe_len_fits(size)) {
                result->fd = file_index;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }

            IoUringOperation* operation = nullptr;
            try {
                operation = io_uring_op_pool_.create();
            } catch (...) {
                result->fd = file_index;
                result->events = io_error;
                result->error = ENOMEM;
                return false;
            }

            operation->task = task;
            operation->result = result;
            operation->complete_events = complete_events;
            operation->direct_file_index = -1;
            operation->opcode = opcode;
            operation->cancel_requested = false;
            operation->multishot = false;
            operation->poll_wait = false;
            operation->zero_copy_send = false;
            operation->zero_copy_primary_done = false;
            operation->zero_copy_notification_done = false;
            operation->msg = nullptr;
            operation->socket_address = nullptr;
            operation->wait_registration = nullptr;

            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                io_uring_op_pool_.destroy(operation);
                result->fd = file_index;
                result->events = io_error;
                result->error = reserve_error == 0 ? EBUSY : reserve_error;
                return false;
            }

            track_io_uring_operation(operation);

            detail::fill_fixed_file_rw_sqe(
                *sqe,
                detail::IoUringFixedFileRwSqe{
                    opcode,
                    file_index,
                    data,
                    size,
                    offset,
                    fixed_buffer_index,
                    fixed_buffer},
                reinterpret_cast<std::uint64_t>(operation));

            result->fd = file_index;
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
        }
#endif
