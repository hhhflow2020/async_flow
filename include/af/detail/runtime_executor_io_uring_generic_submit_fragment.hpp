#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
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
            if (current_thread_index_ != index_ || task == nullptr || result == nullptr) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                }
                return false;
            }
            const bool openat_op = opcode == IORING_OP_OPENAT;
            const bool statx_op = opcode == IORING_OP_STATX;
            const bool renameat_op = opcode == IORING_OP_RENAMEAT;
            const bool unlinkat_op = opcode == IORING_OP_UNLINKAT;
            const bool path_fd_op = openat_op || statx_op || renameat_op || unlinkat_op;
            if (io_uring_fd_ < 0 || (!path_fd_op && fd < 0)) {
                result->fd = fd;
                result->events = io_error;
                result->error = io_uring_fd_ < 0 ? ENOSYS : EBADF;
                return false;
            }
            const bool close_op = opcode == IORING_OP_CLOSE;
            const bool shutdown_op = opcode == IORING_OP_SHUTDOWN;
            const bool fallocate_op = opcode == IORING_OP_FALLOCATE;
            const bool splice_op = opcode == IORING_OP_SPLICE;
            const bool fixed_buffer_op =
                opcode == IORING_OP_READ_FIXED || opcode == IORING_OP_WRITE_FIXED;
            const bool message_op =
                opcode == IORING_OP_RECVMSG ||
                opcode == IORING_OP_SENDMSG ||
                opcode == detail::io_uring_op_sendmsg_zc;
            const bool accept_op = opcode == IORING_OP_ACCEPT;
            const bool connect_op = opcode == IORING_OP_CONNECT;
            const bool address_op = accept_op || connect_op;
            const bool message_iov_op = message_op && message_iov != nullptr;
            const bool accept_address_op =
                accept_op && socket_address_out != nullptr && socket_address_size_out != nullptr;
            const bool needs_socket_address = connect_op || accept_address_op;
            const bool data_optional_op =
                opcode == IORING_OP_FSYNC || close_op || shutdown_op || fallocate_op || splice_op;
            if (!data_optional_op && !address_op && data == nullptr && !message_iov_op &&
                !buffer_select) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (fixed_file) {
                if (path_fd_op) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
                if (!io_uring_files_registered_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENXIO;
                    return false;
                }
                if (static_cast<unsigned>(fd) >= io_uring_registered_file_count_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
            }
            if (direct_file_index >= 0) {
                if (!(openat_op || accept_op)) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
                if (!io_uring_files_registered_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENXIO;
                    return false;
                }
                if (static_cast<unsigned>(direct_file_index) >= io_uring_registered_file_count_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
            }
            if (fixed_buffer_op) {
                if (!io_uring_buffers_registered_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOBUFS;
                    return false;
                }
                if (fixed_buffer_index >= io_uring_registered_buffer_count_) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                    return false;
                }
            }
            if (opcode != IORING_OP_FSYNC && !message_op &&
                size > static_cast<std::size_t>(std::numeric_limits<unsigned>::max())) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (message_iov_op &&
                (message_iov_count == 0U ||
                 message_iov_count > static_cast<std::size_t>(IOV_MAX))) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (connect_op &&
                (socket_address == nullptr ||
                 socket_address_size == 0U ||
                 socket_address_size > static_cast<socklen_t>(sizeof(sockaddr_storage)))) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (accept_op &&
                ((socket_address_out == nullptr) != (socket_address_size_out == nullptr))) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
                return false;
            }
            if (buffer_select &&
                ((opcode != IORING_OP_RECV && opcode != IORING_OP_RECVMSG) ||
                 !provided_buffer_group_registered(provided_buffer_group))) {
                result->fd = fd;
                result->events = io_error;
                result->error =
                    (opcode == IORING_OP_RECV || opcode == IORING_OP_RECVMSG) ? ENOBUFS : EINVAL;
                return false;
            }

            IoUringOperation* operation = nullptr;
            try {
                operation = io_uring_op_pool_.create();
            } catch (...) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOMEM;
                return false;
            }
            operation->task = task;
            operation->result = result;
            operation->complete_events = complete_events;
            operation->direct_file_index = direct_file_index;
            operation->opcode = opcode;
            operation->cancel_requested = false;
            operation->multishot = multishot;
            operation->poll_wait = false;
            operation->zero_copy_send = zero_copy_send;
            operation->zero_copy_primary_done = false;
            operation->zero_copy_notification_done = false;
            operation->msg = nullptr;
            operation->socket_address = nullptr;
            operation->wait_registration = nullptr;
            if (message_op) {
                try {
                    operation->msg = io_uring_msg_pool_.create();
                } catch (...) {
                    io_uring_op_pool_.destroy(operation);
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOMEM;
                    return false;
                }
                operation->msg->header = msghdr{};
                operation->msg->header.msg_name = message_name;
                operation->msg->header.msg_namelen = message_name_len;
                if (opcode == IORING_OP_RECVMSG && multishot && buffer_select) {
                    operation->msg->header.msg_controllen = size;
                } else if (message_iov_op) {
                    operation->msg->header.msg_iov = const_cast<iovec*>(message_iov);
                    operation->msg->header.msg_iovlen = message_iov_count;
                } else {
                    operation->msg->iov = iovec{data, size};
                    operation->msg->header.msg_iov = &operation->msg->iov;
                    operation->msg->header.msg_iovlen = 1;
                }
                operation->msg->address_size = message_name_len_out;
            }
            if (needs_socket_address) {
                try {
                    operation->socket_address = io_uring_address_pool_.create();
                } catch (...) {
                    destroy_io_uring_operation(operation);
                    result->fd = fd;
                    result->events = io_error;
                    result->error = ENOMEM;
                    return false;
                }
                operation->socket_address->storage = sockaddr_storage{};
                operation->socket_address->output = nullptr;
                operation->socket_address->output_size = nullptr;
                operation->socket_address->output_capacity = 0;
                if (connect_op) {
                    std::memcpy(&operation->socket_address->storage, socket_address, socket_address_size);
                    operation->socket_address->size = socket_address_size;
                } else {
                    operation->socket_address->size = sizeof(operation->socket_address->storage);
                    operation->socket_address->output = socket_address_out;
                    operation->socket_address->output_size = socket_address_size_out;
                    operation->socket_address->output_capacity =
                        socket_address_size_out == nullptr ? 0 : *socket_address_size_out;
                }
            }
            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                destroy_io_uring_operation(operation);
                result->fd = fd;
                result->events = io_error;
                result->error = reserve_error == 0 ? EBUSY : reserve_error;
                return false;
            }

            track_io_uring_operation(operation);

            *sqe = io_uring_sqe{};
            sqe->opcode = opcode;
            sqe->fd = fd;
            sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
            if (fixed_file) {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
            if (buffer_select) {
                sqe->flags |= IOSQE_BUFFER_SELECT;
                sqe->buf_index = provided_buffer_group;
            }
            if (direct_file_index >= 0) {
                sqe->file_index = static_cast<std::uint32_t>(direct_file_index) + 1U;
            }
            if (opcode == IORING_OP_FSYNC) {
                sqe->fsync_flags = op_flags;
            } else if (close_op) {
                // fd is already filled.
            } else if (shutdown_op) {
                sqe->len = static_cast<unsigned>(size);
            } else if (fallocate_op) {
                sqe->addr = extra;
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
            } else if (splice_op) {
                sqe->addr = extra;
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
                sqe->splice_fd_in = extra_fd;
                sqe->splice_flags = op_flags;
            } else if (openat_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->open_flags = op_flags;
            } else if (statx_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
                sqe->statx_flags = op_flags;
            } else if (renameat_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
                sqe->rename_flags = op_flags;
            } else if (unlinkat_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->unlink_flags = op_flags;
            } else if (message_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(&operation->msg->header);
                sqe->len = 1U;
                sqe->msg_flags = op_flags;
                if (opcode == IORING_OP_RECVMSG && multishot) {
                    sqe->ioprio |= IORING_RECV_MULTISHOT;
                }
            } else if (accept_op) {
                if (operation->socket_address != nullptr) {
                    sqe->addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
                    sqe->addr2 = reinterpret_cast<std::uint64_t>(&operation->socket_address->size);
                }
                sqe->accept_flags = op_flags;
                if (multishot) {
                    sqe->ioprio |= IORING_ACCEPT_MULTISHOT;
                }
            } else if (connect_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
                sqe->off = operation->socket_address->size;
            } else if (fixed_buffer_op) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
                sqe->buf_index = fixed_buffer_index;
            } else if (opcode == IORING_OP_RECV ||
                       opcode == IORING_OP_SEND ||
                       opcode == detail::io_uring_op_send_zc) {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->msg_flags = op_flags;
                if (opcode == IORING_OP_RECV && multishot) {
                    sqe->ioprio |= IORING_RECV_MULTISHOT;
                }
            } else {
                sqe->addr = reinterpret_cast<std::uint64_t>(data);
                sqe->len = static_cast<unsigned>(size);
                sqe->off = offset;
            }

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
