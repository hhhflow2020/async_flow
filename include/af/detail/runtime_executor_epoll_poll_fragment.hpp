#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_poll_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
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
        }
