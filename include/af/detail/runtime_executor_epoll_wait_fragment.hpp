#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_wait_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_native_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            bool prefer_rearm) noexcept {
            static_cast<void>(prefer_rearm);
            if (io_epoll_fd_ < 0 || fd < 0 || events == 0U ||
                io_waits_.find(fd) != io_waits_.end()) {
                result->fd = fd;
                result->events = io_error;
                if (fd < 0) {
                    result->error = EBADF;
                } else if (events == 0U) {
                    result->error = EINVAL;
                } else if (io_epoll_fd_ < 0) {
                    result->error = ENOSYS;
                } else {
                    result->error = EALREADY;
                }
                return false;
            }

            IoWaitRegistration* registration = nullptr;
            try {
                registration = io_wait_pool_.create();
                auto [it, inserted] = io_waits_.emplace(fd, registration);
                static_cast<void>(it);
                if (!inserted) {
                    io_wait_pool_.destroy(registration);
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EALREADY;
                    return false;
                }
            } catch (...) {
                if (registration != nullptr) {
                    io_wait_pool_.destroy(registration);
                }
                result->fd = fd;
                result->events = io_error;
                result->error = ENOMEM;
                return false;
            }
            registration->fd = fd;
            registration->events = events;
            registration->task = task;
            registration->result = result;
            registration->poll_operation = nullptr;

            const IoUringPollSubmitResult poll_result =
                try_submit_io_uring_poll_wait(fd, events, task, result, registration);
            if (poll_result == IoUringPollSubmitResult::Submitted) {
                *result = IoResult{fd, 0, 0};
                return true;
            }
            if (poll_result == IoUringPollSubmitResult::Failed) {
                io_waits_.erase(fd);
                io_wait_pool_.destroy(registration);
                return false;
            }
            if (poll_result == IoUringPollSubmitResult::BackendClosed) {
                return false;
            }

            std::uint32_t native_events = EPOLLERR | EPOLLHUP | EPOLLONESHOT;
            if ((events & io_readable) != 0U) {
                native_events |= EPOLLIN;
            }
            if ((events & io_writable) != 0U) {
                native_events |= EPOLLOUT;
            }

            epoll_event event{};
            event.events = native_events;
            event.data.ptr = registration;

            if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
                const int first_error = errno;
                if (first_error != EEXIST ||
                    ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) != 0) {
                    io_waits_.erase(fd);
                    io_wait_pool_.destroy(registration);
                    result->fd = fd;
                    result->events = io_error;
                    result->error = errno;
                    return false;
                }
            }

            *result = IoResult{fd, 0, 0};
            return true;
        }
