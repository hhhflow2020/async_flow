#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_poll_helpers_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void drain_io_wake() noexcept {
            std::uint64_t value = 0;
            while (::read(io_wake_fd_, &value, sizeof(value)) == sizeof(value)) {
            }
            io_wake_pending_.store(false, std::memory_order_release);
        }

        [[nodiscard]] static std::uint32_t native_poll_events(std::uint32_t events) noexcept {
            std::uint32_t result = POLLERR | POLLHUP;
            if ((events & io_readable) != 0U) {
                result |= POLLIN;
            }
            if ((events & io_writable) != 0U) {
                result |= POLLOUT;
            }
            return result;
        }

        [[nodiscard]] static std::uint32_t io_events_from_poll(std::uint32_t events) noexcept {
            std::uint32_t result = 0;
            if ((events & (POLLIN | POLLPRI)) != 0U) {
                result |= io_readable;
            }
            if ((events & POLLOUT) != 0U) {
                result |= io_writable;
            }
            if ((events & (POLLERR | POLLNVAL)) != 0U) {
                result |= io_error;
            }
            if ((events & POLLHUP) != 0U) {
                result |= io_hangup;
            }
#ifdef POLLRDHUP
            if ((events & POLLRDHUP) != 0U) {
                result |= io_hangup;
            }
#endif
            return result;
        }

        [[nodiscard]] static std::uint32_t io_events_from_native(std::uint32_t events) noexcept {
            std::uint32_t result = 0;
            if ((events & EPOLLIN) != 0U) {
                result |= io_readable;
            }
            if ((events & EPOLLOUT) != 0U) {
                result |= io_writable;
            }
            if ((events & EPOLLERR) != 0U) {
                result |= io_error;
            }
            if ((events & EPOLLHUP) != 0U) {
                result |= io_hangup;
            }
            return result;
        }
#endif
