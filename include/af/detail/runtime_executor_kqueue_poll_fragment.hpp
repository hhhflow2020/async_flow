#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_poll_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
            if (io_kqueue_fd_ < 0) {
                return did_work;
            }
            if (timeout_ms == 0 && io_waits_.empty() && io_kqueue_timeout_count_ == 0U &&
                !io_wake_pending_.load(std::memory_order_acquire)) {
                return did_work;
            }

            timespec timeout {};
            timespec* timeout_ptr = nullptr;
            if (timeout_ms >= 0) {
                timeout.tv_sec = timeout_ms / 1000;
                timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
                timeout_ptr = &timeout;
            }

            std::array<struct kevent, 64> events;
            const int count = ::kevent(
                io_kqueue_fd_,
                nullptr,
                0,
                events.data(),
                static_cast<int>(events.size()),
                timeout_ptr);
            if (count <= 0) {
                return did_work;
            }

            std::array<IoWaitRegistration*, 64> completed{};
            std::size_t completed_count = 0;
            for (int i = 0; i < count; ++i) {
                const struct kevent& event = events[static_cast<std::size_t>(i)];
                if (event.filter == EVFILT_USER) {
                    io_wake_pending_.store(false, std::memory_order_release);
                    did_work = true;
                    continue;
                }
                if (event.filter == EVFILT_TIMER) {
                    auto* timeout = static_cast<KqueueTimeoutRegistration*>(event.udata);
                    if (complete_kqueue_timeout(timeout, event)) {
                        did_work = true;
                    }
                    continue;
                }

                auto* registration = static_cast<IoWaitRegistration*>(event.udata);
                if (registration == nullptr) {
                    continue;
                }

                const int fd = registration->fd;
                auto it = io_waits_.find(fd);
                if (it == io_waits_.end() || it->second != registration) {
                    continue;
                }

                remove_kqueue_filters(*registration);
                io_waits_.erase(it);

                registration->result->fd = fd;
                registration->result->events = io_events_from_kqueue(event);
                registration->result->error = io_error_from_kqueue(event);
                enqueue_pending_blocking(index_, registration->task);
                completed[completed_count++] = registration;
                did_work = true;
            }

            for (std::size_t i = 0; i < completed_count; ++i) {
                io_wait_pool_.destroy(completed[i]);
            }
            return did_work;
        }
