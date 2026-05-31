#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_timeout_completion_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool cancel_kqueue_timeout(IoOpState& state) noexcept {
            if (io_kqueue_fd_ < 0) {
                state.wait.events = io_error;
                state.wait.error = ENOSYS;
                state.wait.result = -ENOSYS;
                return false;
            }

            auto* registration = static_cast<KqueueTimeoutRegistration*>(
                state.wait.completion_token);
            if (registration == nullptr || registration->result != &state.wait) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }

            struct kevent event {};
            EV_SET(&event, registration->ident, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0 &&
                errno != ENOENT) {
                const int error = errno == 0 ? EIO : errno;
                state.wait.events = io_error;
                state.wait.error = error;
                state.wait.result = -error;
                return false;
            }

            untrack_kqueue_timeout(registration);
            state.wait.fd = -1;
            state.wait.events = io_error;
            state.wait.error = ECANCELED;
            state.wait.result = -ECANCELED;
            state.wait.completion_token = nullptr;
            if (registration->task != running_task_) {
                enqueue_pending_blocking(index_, registration->task);
            }
            io_kqueue_timeout_pool_.destroy(registration);
            return true;
        }

        [[nodiscard]] bool complete_kqueue_timeout(
            KqueueTimeoutRegistration* registration,
            const struct kevent& event) noexcept {
            if (registration == nullptr || registration->result == nullptr) {
                return false;
            }

            IoResult* result = registration->result;
            if (result->completion_token != registration) {
                return false;
            }

            untrack_kqueue_timeout(registration);
            result->fd = -1;
            result->events = io_error;
            result->error = io_error_from_kqueue(event);
            if (result->error == 0) {
                result->error = ETIMEDOUT;
            }
            result->result = -result->error;
            result->completion_token = nullptr;
            enqueue_pending_blocking(index_, registration->task);
            io_kqueue_timeout_pool_.destroy(registration);
            return true;
        }
