#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_core_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool submit_io_uring_op(
            std::uint8_t opcode,
            int fd,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint32_t op_flags,
            std::uint32_t complete_events,
            Task* task,
            IoResult* result,
            sockaddr* message_name = nullptr,
            socklen_t message_name_len = 0,
            socklen_t* message_name_len_out = nullptr,
            const sockaddr* socket_address = nullptr,
            socklen_t socket_address_size = 0,
            sockaddr* socket_address_out = nullptr,
            socklen_t* socket_address_size_out = nullptr,
            const iovec* message_iov = nullptr,
            std::size_t message_iov_count = 0,
            std::uint64_t extra = 0,
            std::int32_t extra_fd = -1,
            std::uint16_t fixed_buffer_index = 0,
            bool fixed_file = false,
            bool multishot = false,
            bool zero_copy_send = false,
            std::uint16_t provided_buffer_group = 0,
            bool buffer_select = false,
            int direct_file_index = -1) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring submit must be called from its IO thread");
            if (result != nullptr) {
                result->completion_token = nullptr;
            }

            const IoUringGenericSubmitArgs args{
                opcode,
                fd,
                data,
                size,
                offset,
                op_flags,
                complete_events,
                task,
                result,
                message_name,
                message_name_len,
                message_name_len_out,
                socket_address,
                socket_address_size,
                socket_address_out,
                socket_address_size_out,
                message_iov,
                message_iov_count,
                extra,
                extra_fd,
                fixed_buffer_index,
                fixed_file,
                multishot,
                zero_copy_send,
                provided_buffer_group,
                buffer_select,
                direct_file_index};
            const IoUringGenericSubmitKind kind = classify_io_uring_generic_submit(args);
            if (!validate_io_uring_generic_submit(args, kind)) {
                return false;
            }

            IoUringOperation* operation = create_io_uring_generic_submit_operation(args, kind);
            if (operation == nullptr) {
                return false;
            }

            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                destroy_io_uring_operation(operation);
                set_io_uring_generic_submit_error(
                    result,
                    fd,
                    reserve_error == 0 ? EBUSY : reserve_error);
                return false;
            }

            track_io_uring_operation(operation);
            fill_io_uring_generic_submit_sqe(*sqe, args, kind, operation);

            result->fd = fd;
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
