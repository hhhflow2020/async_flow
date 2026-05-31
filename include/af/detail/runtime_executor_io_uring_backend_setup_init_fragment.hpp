#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_init_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void init_io_uring_backend() noexcept {
            if (io_uring_fd_ >= 0 || io_wake_fd_ < 0) {
                return;
            }

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
                return;
            }

            if (!map_io_uring_rings(params)) {
                close_io_uring_backend();
                return;
            }

            bind_io_uring_ring_pointers(params);
            detect_io_uring_features();

            if (!register_io_uring_wake_fd()) {
                close_io_uring_backend();
            }
        }
#endif
