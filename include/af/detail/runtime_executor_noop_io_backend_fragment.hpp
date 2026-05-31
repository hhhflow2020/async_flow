#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_noop_io_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool native_io_backend_available() const noexcept {
            return false;
        }

        [[nodiscard]] bool notify_native_io_backend() noexcept {
            return false;
        }

        [[nodiscard]] bool init_native_io_backend() noexcept {
            return false;
        }

        void close_native_io_backend() noexcept {}

        [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
            static_cast<void>(timeout_ms);
            return did_work;
        }

        [[nodiscard]] bool register_native_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            bool prefer_rearm) noexcept {
            static_cast<void>(events);
            static_cast<void>(task);
            static_cast<void>(prefer_rearm);
            result->fd = fd;
            result->events = io_error;
            result->error = fd < 0 ? EBADF : ENOSYS;
            return false;
        }
