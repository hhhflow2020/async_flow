#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_storage_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void flush_native_io_backend_deferred_deletes() noexcept {}

        void clear_io_waits() noexcept {
            for (auto& entry : io_waits_) {
                io_wait_pool_.destroy(entry.second);
            }
            io_waits_.clear();
        }

        void reserve_native_io_wait_storage() noexcept {
            try {
                if constexpr (io_wait_reserve != 0U) {
                    io_waits_.reserve(io_wait_reserve);
                }
            } catch (...) {
            }
        }
