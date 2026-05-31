#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_storage_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void reserve_io_backend_storage() noexcept {
            try {
                if constexpr (io_wait_reserve != 0U) {
                    io_waits_.reserve(io_wait_reserve);
                }
                if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
                    io_uring_provided_buffer_groups_.reserve(
                        io_uring_provided_buffer_group_reserve);
                }
            } catch (...) {
            }
        }
#endif
