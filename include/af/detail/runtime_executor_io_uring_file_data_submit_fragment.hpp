#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_data_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_read(
            int fd,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_buffer_op(
                IORING_OP_READ,
                fd,
                data,
                size,
                offset,
                0,
                io_readable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_write(
            int fd,
            const void* data,
            std::size_t size,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_buffer_op(
                IORING_OP_WRITE,
                fd,
                const_cast<void*>(data),
                size,
                offset,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
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

        [[nodiscard]] bool submit_io_uring_read_fixed_file(
            int file_index,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_READ,
                file_index,
                data,
                size,
                offset,
                io_readable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_write_fixed_file(
            int file_index,
            const void* data,
            std::size_t size,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_WRITE,
                file_index,
                const_cast<void*>(data),
                size,
                offset,
                io_writable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

#if !defined(_WIN32)
        [[nodiscard]] bool submit_io_uring_readv_fixed_file(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_READV,
                file_index,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                io_readable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_writev_fixed_file(
            int file_index,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_WRITEV,
                file_index,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                io_writable,
                task,
                result);
#else
            static_cast<void>(file_index);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_read_fixed_file(
            int file_index,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_READ_FIXED,
                file_index,
                data,
                size,
                offset,
                io_readable,
                task,
                result,
                buffer_index,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_write_fixed_file(
            int file_index,
            const void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_WRITE_FIXED,
                file_index,
                const_cast<void*>(data),
                size,
                offset,
                io_writable,
                task,
                result,
                buffer_index,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_read_fixed(
            int fd,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_READ_FIXED,
                fd,
                data,
                size,
                offset,
                0,
                io_readable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                buffer_index,
                false);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_write_fixed(
            int fd,
            const void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_WRITE_FIXED,
                fd,
                const_cast<void*>(data),
                size,
                offset,
                0,
                io_writable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                buffer_index,
                false);
#else
            static_cast<void>(fd);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_readv(
            int fd,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_READV,
                fd,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                0,
                io_readable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_writev(
            int fd,
            const iovec* iov,
            int iov_count,
            std::uint64_t offset,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_WRITEV,
                fd,
                const_cast<iovec*>(iov),
                static_cast<std::size_t>(iov_count),
                offset,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(iov);
            static_cast<void>(iov_count);
            static_cast<void>(offset);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
#endif

        [[nodiscard]] bool submit_io_uring_fsync(
            int fd,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(IORING_OP_FSYNC, fd, nullptr, 0, 0, flags, io_writable, task, result);
#else
            static_cast<void>(fd);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_fsync_fixed_file(
            int file_index,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_FSYNC,
                file_index,
                nullptr,
                0,
                0,
                flags,
                io_writable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                0,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

