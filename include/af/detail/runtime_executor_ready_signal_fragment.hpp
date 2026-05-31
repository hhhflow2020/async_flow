#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_ready_signal_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void mark_ready(std::uint16_t source) noexcept {
            ready_sources_.mark(source);
        }

        void notify_external_ready() noexcept {
            if (!external_ready_.load(std::memory_order_acquire)) {
                external_ready_.store(true, std::memory_order_release);
                notify_force();
                return;
            }
            notify();
        }
