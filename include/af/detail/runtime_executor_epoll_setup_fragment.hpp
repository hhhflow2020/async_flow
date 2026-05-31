#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_setup_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool native_io_backend_available() const noexcept {
            return io_epoll_fd_ >= 0;
        }

        [[nodiscard]] bool notify_native_io_backend() noexcept {
            if (io_epoll_fd_ < 0) {
                return false;
            }
            bool expected = false;
            if (io_wake_pending_.compare_exchange_strong(
                    expected,
                    true,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                const std::uint64_t value = 1;
                const auto written = ::write(io_wake_fd_, &value, sizeof(value));
                static_cast<void>(written);
            }
            return true;
        }

        [[nodiscard]] bool init_native_io_backend() noexcept {
            if (!native_io_thread()) {
                return false;
            }
            if (io_epoll_fd_ >= 0) {
                return true;
            }
            reserve_io_backend_storage();

            io_epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (io_epoll_fd_ < 0) {
                return false;
            }

            io_wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (io_wake_fd_ < 0) {
                close_native_io_backend();
                return false;
            }

            epoll_event event{};
            event.events = EPOLLIN;
            event.data.ptr = nullptr;
            if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
                close_native_io_backend();
                return false;
            }
            return true;
        }

        void close_native_io_backend() noexcept {
            clear_io_waits();
            if (io_wake_fd_ >= 0) {
                ::close(io_wake_fd_);
                io_wake_fd_ = -1;
            }
            if (io_epoll_fd_ >= 0) {
                ::close(io_epoll_fd_);
                io_epoll_fd_ = -1;
            }
            io_wake_pending_.store(false, std::memory_order_relaxed);
        }
