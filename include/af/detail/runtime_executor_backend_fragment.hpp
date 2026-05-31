#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void notify_force() noexcept {
#if defined(__linux__)
            if (io_epoll_fd_ >= 0) {
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
                return;
            }
#endif
            wake_epoch_.fetch_add(1, std::memory_order_release);
            wake_epoch_.notify_one();
        }

        void run_loop() noexcept {
            current_thread_index_ = index_;

            for (;;) {
                bool did_work = false;
                while (Task* task = pop_one()) {
                    did_work = true;
                    execute(task);
                }

#if defined(__linux__)
                if (flush_io_uring_submissions_or_fail()) {
                    did_work = true;
                }
                flush_deferred_io_deletes();
#endif

                if (stop_requested_.load(std::memory_order_acquire)) {
                    if (!did_work) {
                        break;
                    }
                    continue;
                }

                if (poll_io(0)) {
                    continue;
                }

                const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
                sleeping_.store(true, std::memory_order_release);
                if (stop_requested_.load(std::memory_order_acquire)) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    continue;
                }

                if (Task* task = pop_one()) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    execute(task);
                } else {
                    if (io_thread() && io_backend_available()) {
                        static_cast<void>(poll_io(-1));
                    } else if (wake_epoch_.load(std::memory_order_acquire) == observed) {
                        wake_epoch_.wait(observed, std::memory_order_acquire);
                    }
                    sleeping_.store(false, std::memory_order_relaxed);
                }
            }

            current_thread_index_ = invalid_thread_index;
        }

        void init_io_backend() noexcept {
#if defined(__linux__)
            if (!io_thread() || io_epoll_fd_ >= 0) {
                return;
            }
            reserve_io_backend_storage();

            io_epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (io_epoll_fd_ < 0) {
                return;
            }

            io_wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (io_wake_fd_ < 0) {
                close_io_backend();
                return;
            }

            epoll_event event{};
            event.events = EPOLLIN;
            event.data.ptr = nullptr;
            if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
                close_io_backend();
                return;
            }

            if (io_uring_thread()) {
                init_io_uring_backend();
            }
#endif
        }

        void close_io_backend() noexcept {
#if defined(__linux__)
            close_io_uring_backend();
            clear_io_waits();
            io_deferred_deletes_.clear();
            if (io_wake_fd_ >= 0) {
                ::close(io_wake_fd_);
                io_wake_fd_ = -1;
            }
            if (io_epoll_fd_ >= 0) {
                ::close(io_epoll_fd_);
                io_epoll_fd_ = -1;
            }
            io_wake_pending_.store(false, std::memory_order_relaxed);
#endif
        }

        [[nodiscard]] bool poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
            bool did_work = poll_io_uring_completions();
            if (io_epoll_fd_ < 0) {
                return did_work;
            }
            if (timeout_ms == 0 && io_waits_.empty()) {
                return did_work;
            }

            std::array<epoll_event, 64> events;
            const int count = ::epoll_wait(
                io_epoll_fd_,
                events.data(),
                static_cast<int>(events.size()),
                timeout_ms);
            if (count <= 0) {
                return did_work;
            }

            for (int i = 0; i < count; ++i) {
                auto* registration = static_cast<IoWaitRegistration*>(
                    events[static_cast<std::size_t>(i)].data.ptr);
                if (registration == nullptr) {
                    drain_io_wake();
                    if (poll_io_uring_completions()) {
                        did_work = true;
                    }
                    did_work = true;
                    continue;
                }

                const int fd = registration->fd;
                io_waits_.erase(fd);
                defer_io_delete(fd);

                registration->result->fd = fd;
                registration->result->events = io_events_from_native(
                    events[static_cast<std::size_t>(i)].events);
                registration->result->error = 0;
                enqueue_pending_blocking(index_, registration->task);
                io_wait_pool_.destroy(registration);
                did_work = true;
            }
            return did_work;
#else
            static_cast<void>(timeout_ms);
            return false;
#endif
        }

#if defined(__linux__)
        void defer_io_delete(int fd) {
            io_deferred_deletes_.insert(fd);
        }

        void forget_deferred_io_delete(int fd) noexcept {
            io_deferred_deletes_.erase(fd);
        }

        void flush_deferred_io_deletes() noexcept {
            if (io_deferred_deletes_.empty()) {
                return;
            }
            for (int fd : io_deferred_deletes_) {
                static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
            }
            io_deferred_deletes_.clear();
        }

        void clear_io_waits() noexcept {
            for (auto& entry : io_waits_) {
                io_wait_pool_.destroy(entry.second);
            }
            io_waits_.clear();
        }
#endif
