#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_types_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        struct IoUringGenericSubmitArgs {
            std::uint8_t opcode;
            int fd;
            void* data;
            std::size_t size;
            std::uint64_t offset;
            std::uint32_t op_flags;
            std::uint32_t complete_events;
            Task* task;
            IoResult* result;
            sockaddr* message_name;
            socklen_t message_name_len;
            socklen_t* message_name_len_out;
            const sockaddr* socket_address;
            socklen_t socket_address_size;
            sockaddr* socket_address_out;
            socklen_t* socket_address_size_out;
            const iovec* message_iov;
            std::size_t message_iov_count;
            std::uint64_t extra;
            std::int32_t extra_fd;
            std::uint16_t fixed_buffer_index;
            bool fixed_file;
            bool multishot;
            bool zero_copy_send;
            std::uint16_t provided_buffer_group;
            bool buffer_select;
            int direct_file_index;
        };

        struct IoUringGenericSubmitKind {
            bool openat_op;
            bool statx_op;
            bool renameat_op;
            bool unlinkat_op;
            bool path_fd_op;
            bool close_op;
            bool shutdown_op;
            bool fallocate_op;
            bool splice_op;
            bool fixed_buffer_op;
            bool message_op;
            bool accept_op;
            bool connect_op;
            bool address_op;
            bool message_iov_op;
            bool accept_address_op;
            bool needs_socket_address;
            bool data_optional_op;
        };

        static void set_io_uring_generic_submit_error(
            IoResult* result,
            int fd,
            int error) noexcept {
            if (result == nullptr) {
                return;
            }
            result->fd = fd;
            result->events = io_error;
            result->error = error;
        }

        [[nodiscard]] static IoUringGenericSubmitKind classify_io_uring_generic_submit(
            const IoUringGenericSubmitArgs& args) noexcept {
            const bool openat_op = args.opcode == IORING_OP_OPENAT;
            const bool statx_op = args.opcode == IORING_OP_STATX;
            const bool renameat_op = args.opcode == IORING_OP_RENAMEAT;
            const bool unlinkat_op = args.opcode == IORING_OP_UNLINKAT;
            const bool close_op = args.opcode == IORING_OP_CLOSE;
            const bool shutdown_op = args.opcode == IORING_OP_SHUTDOWN;
            const bool fallocate_op = args.opcode == IORING_OP_FALLOCATE;
            const bool splice_op = args.opcode == IORING_OP_SPLICE;
            const bool fixed_buffer_op =
                args.opcode == IORING_OP_READ_FIXED || args.opcode == IORING_OP_WRITE_FIXED;
            const bool message_op =
                args.opcode == IORING_OP_RECVMSG ||
                args.opcode == IORING_OP_SENDMSG ||
                args.opcode == detail::io_uring_op_sendmsg_zc;
            const bool accept_op = args.opcode == IORING_OP_ACCEPT;
            const bool connect_op = args.opcode == IORING_OP_CONNECT;
            const bool message_iov_op = message_op && args.message_iov != nullptr;
            const bool accept_address_op =
                accept_op && args.socket_address_out != nullptr && args.socket_address_size_out != nullptr;
            return IoUringGenericSubmitKind{
                openat_op,
                statx_op,
                renameat_op,
                unlinkat_op,
                openat_op || statx_op || renameat_op || unlinkat_op,
                close_op,
                shutdown_op,
                fallocate_op,
                splice_op,
                fixed_buffer_op,
                message_op,
                accept_op,
                connect_op,
                accept_op || connect_op,
                message_iov_op,
                accept_address_op,
                connect_op || accept_address_op,
                args.opcode == IORING_OP_FSYNC || close_op || shutdown_op || fallocate_op || splice_op};
        }
#endif
