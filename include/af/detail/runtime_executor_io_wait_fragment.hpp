#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_wait_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            bool prefer_rearm = false) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_wait must be called from its IO thread");
            if (current_thread_index_ != index_ || task == nullptr || result == nullptr) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                }
                return false;
            }

#if defined(__linux__)
            if (io_epoll_fd_ < 0 || fd < 0 || events == 0U || io_waits_.find(fd) != io_waits_.end()) {
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
            registration->task = task;
            registration->result = result;
            registration->poll_operation = nullptr;

            const IoUringPollSubmitResult poll_result =
                try_submit_io_uring_poll_wait(fd, events, task, result, registration);
            if (poll_result == IoUringPollSubmitResult::Submitted) {
                forget_deferred_io_delete(fd);
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

            const int first_op = prefer_rearm ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
            const int fallback_op = prefer_rearm ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
            const int fallback_error = prefer_rearm ? ENOENT : EEXIST;
            if (::epoll_ctl(io_epoll_fd_, first_op, fd, &event) != 0) {
                const int first_error = errno;
                if (first_error != fallback_error ||
                    ::epoll_ctl(io_epoll_fd_, fallback_op, fd, &event) != 0) {
                    io_waits_.erase(fd);
                    io_wait_pool_.destroy(registration);
                    result->fd = fd;
                    result->events = io_error;
                    result->error = errno;
                    return false;
                }
            }

            forget_deferred_io_delete(fd);
            *result = IoResult{fd, 0, 0};
            return true;
#else
            static_cast<void>(fd);
            static_cast<void>(events);
            static_cast<void>(task);
            result->error = ENOSYS;
            return false;
#endif
        }

#if defined(__linux__)
        [[nodiscard]] bool cancel_io_wait(IoOpState& state) noexcept {
            if (io_epoll_fd_ < 0) {
                state.wait.events = io_error;
                state.wait.error = ENOSYS;
                state.wait.result = -ENOSYS;
                return false;
            }

            const int fd = state.wait.fd;
            auto it = io_waits_.find(fd);
            if (fd < 0 || it == io_waits_.end() || it->second->result != &state.wait) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }

            IoWaitRegistration* registration = it->second;
            if (registration->poll_operation != nullptr) {
                IoUringOperation* operation = registration->poll_operation;
                const int submit_error = submit_io_uring_cancel(operation);
                if (submit_error != 0) {
                    state.wait.events = io_error;
                    state.wait.error = submit_error;
                    state.wait.result = -submit_error;
                    return false;
                }

                io_waits_.erase(it);
                registration->poll_operation = nullptr;
                if (operation->wait_registration == registration) {
                    operation->wait_registration = nullptr;
                }
                operation->cancel_requested = true;
                operation->task = nullptr;
                operation->result = nullptr;
                state.readiness_rearm_hint = false;
                state.readiness_fd = -1;

                state.wait.fd = fd;
                state.wait.events = io_error;
                state.wait.error = ECANCELED;
                state.wait.result = -ECANCELED;
                if (registration->task != running_task_) {
                    enqueue_pending_blocking(index_, registration->task);
                }
                io_wait_pool_.destroy(registration);
                return true;
            }
            io_waits_.erase(it);
            static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
            forget_deferred_io_delete(fd);
            state.readiness_rearm_hint = false;
            state.readiness_fd = -1;

            state.wait.fd = fd;
            state.wait.events = io_error;
            state.wait.error = ECANCELED;
            state.wait.result = -ECANCELED;
            if (registration->task != running_task_) {
                enqueue_pending_blocking(index_, registration->task);
            }
            io_wait_pool_.destroy(registration);
            return true;
        }

        [[nodiscard]] bool cancel_io_completion(IoOpState& state) noexcept {
            if (io_uring_fd_ < 0) {
                state.wait.events = io_error;
                state.wait.error = ENOSYS;
                state.wait.result = -ENOSYS;
                return false;
            }

            auto* operation = static_cast<IoUringOperation*>(state.wait.completion_token);
            if (operation == nullptr || operation->result != &state.wait || operation->poll_wait) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }
            if (operation->opcode == IORING_OP_CLOSE) {
                state.wait.events = io_error;
                state.wait.error = EOPNOTSUPP;
                state.wait.result = -EOPNOTSUPP;
                return false;
            }

            const int submit_error = submit_io_uring_cancel(operation);
            if (submit_error != 0) {
                state.wait.events = io_error;
                state.wait.error = submit_error;
                state.wait.result = -submit_error;
                return false;
            }

            operation->cancel_requested = true;
            state.wait.events = io_error;
            state.wait.error = ECANCELED;
            state.wait.result = -ECANCELED;
            return true;
        }
#endif

        [[nodiscard]] bool cancel_io(IoOpState& state) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "cancel_io must be called from its IO thread");
            if (state.waiting && state.wait.error == ECANCELED) {
                return true;
            }
            if (current_thread_index_ != index_) {
                state.wait.events = io_error;
                state.wait.error = EINVAL;
                state.wait.result = -EINVAL;
                return false;
            }
            if (!state.waiting) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }

#if defined(__linux__)
            if (state.wait_kind == IoWaitKind::Readiness) {
                return cancel_io_wait(state);
            }
            if (state.wait_kind == IoWaitKind::Completion) {
                return cancel_io_completion(state);
            }
#endif
            state.wait.events = io_error;
            state.wait.error = ENOSYS;
            state.wait.result = -ENOSYS;
            return false;
        }
