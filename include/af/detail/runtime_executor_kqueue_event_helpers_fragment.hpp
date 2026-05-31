#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_event_helpers_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] static std::uint32_t io_events_from_kqueue(
            const struct kevent& event) noexcept {
            if ((event.flags & EV_ERROR) != 0) {
                return io_error;
            }

            std::uint32_t result = 0;
            if (event.filter == EVFILT_READ) {
                result |= io_readable;
            } else if (event.filter == EVFILT_WRITE) {
                result |= io_writable;
            }
            if ((event.flags & EV_EOF) != 0) {
                result |= io_hangup;
            }
            return result;
        }

        [[nodiscard]] static int io_error_from_kqueue(
            const struct kevent& event) noexcept {
            if ((event.flags & EV_ERROR) == 0) {
                return 0;
            }
            return event.data == 0 ? EIO : static_cast<int>(event.data);
        }
