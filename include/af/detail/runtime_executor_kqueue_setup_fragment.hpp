#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_setup_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        static constexpr uintptr_t kqueue_wake_ident = 1;

        [[nodiscard]] bool native_io_backend_available() const noexcept {
            return io_kqueue_fd_ >= 0;
        }

        [[nodiscard]] bool notify_native_io_backend() noexcept {
            if (io_kqueue_fd_ < 0) {
                return false;
            }
            bool expected = false;
            if (!io_wake_pending_.compare_exchange_strong(
                    expected,
                    true,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return true;
            }

            struct kevent event {};
            EV_SET(&event, kqueue_wake_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
                io_wake_pending_.store(false, std::memory_order_release);
                return false;
            }
            return true;
        }

        [[nodiscard]] bool init_native_io_backend() noexcept {
            if (!native_io_thread()) {
                return false;
            }
            if (io_kqueue_fd_ >= 0) {
                return true;
            }
            reserve_native_io_wait_storage();

            io_kqueue_fd_ = ::kqueue();
            if (io_kqueue_fd_ < 0) {
                return false;
            }

            struct kevent event {};
            EV_SET(&event, kqueue_wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
                close_native_io_backend();
                return false;
            }
            return true;
        }

        void close_native_io_backend() noexcept {
            clear_io_waits();
            clear_kqueue_timeouts();
            if (io_kqueue_fd_ >= 0) {
                ::close(io_kqueue_fd_);
                io_kqueue_fd_ = -1;
            }
            io_wake_pending_.store(false, std::memory_order_relaxed);
        }
