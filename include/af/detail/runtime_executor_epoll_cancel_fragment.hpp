#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_cancel_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool cancel_native_io_wait(IoOpState& state) noexcept {
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
