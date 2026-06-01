#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_notify_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void notify() noexcept {
            wake_epoch_.fetch_add(1, std::memory_order_release);
            if (!io_thread() || !io_backend_available()) {
                wake_epoch_.notify_one();
                return;
            }

            if (!sleeping_.load(std::memory_order_acquire)) {
                return;
            }

            bool expected = true;
            if (sleeping_.compare_exchange_strong(
                    expected,
                    false,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                if (notify_native_io_backend()) {
                    return;
                }
                wake_epoch_.notify_one();
            }
        }
