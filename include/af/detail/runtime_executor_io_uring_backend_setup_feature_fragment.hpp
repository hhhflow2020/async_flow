#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_feature_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void detect_io_uring_features() noexcept {
            io_uring_send_zc_available_ = false;
            io_uring_sendmsg_zc_available_ = false;
            io_uring_poll_add_available_ = false;
            io_uring_socket_available_ = false;

            constexpr unsigned probe_count = 64;
            std::array<
                std::byte,
                sizeof(io_uring_probe) + probe_count * sizeof(io_uring_probe_op)>
                storage{};
            auto* probe = reinterpret_cast<io_uring_probe*>(storage.data());
            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_PROBE,
                    probe,
                    probe_count) != 0) {
                return;
            }

            const auto* ops = reinterpret_cast<const io_uring_probe_op*>(
                storage.data() + sizeof(io_uring_probe));
            const unsigned op_count = std::min<unsigned>(probe->ops_len, probe_count);
            for (unsigned i = 0; i < op_count; ++i) {
                if (ops[i].op == detail::io_uring_op_send_zc &&
                    (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_send_zc_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_sendmsg_zc &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_sendmsg_zc_available_ = true;
                } else if (ops[i].op == IORING_OP_POLL_ADD &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_poll_add_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_socket &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_socket_available_ = true;
                }
            }
        }
#endif
