#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_completion_cancel_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
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
