#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_storage_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void clear_io_waits() noexcept {
            for (auto& entry : io_waits_) {
                io_wait_pool_.destroy(entry.second);
            }
            io_waits_.clear();
        }
