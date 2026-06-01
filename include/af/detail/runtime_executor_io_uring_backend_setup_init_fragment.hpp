#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_init_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void init_io_uring_backend() noexcept {
            if (io_uring_fd_ >= 0) {
                return;
            }
            if (io_wake_fd_ < 0) {
                io_uring_backend_error_ = ENODEV;
                return;
            }

            io_uring_backend_error_ = 0;
            io_uring_params params{};
            detail::configure_io_uring_params(
                params,
                detail::IoUringSetupRequest{
                    io_uring_requested_setup_flags(),
                    io_uring_cq_entries,
                    io_uring_sqpoll_idle_ms,
                    io_uring_sqpoll_cpu});
            io_uring_fd_ = detail::sys_io_uring_setup(io_uring_entries, &params);
            if (io_uring_fd_ < 0) {
                io_uring_backend_error_ = errno == 0 ? EIO : errno;
                return;
            }

            if (!map_io_uring_rings(params)) {
                const int map_error = errno == 0 ? EIO : errno;
                close_io_uring_backend();
                io_uring_backend_error_ = map_error;
                return;
            }

            bind_io_uring_ring_pointers(params);
            detect_io_uring_features();

            if (!register_io_uring_wake_fd()) {
                const int register_error = errno == 0 ? EIO : errno;
                close_io_uring_backend();
                io_uring_backend_error_ = register_error;
            }
        }
#endif
