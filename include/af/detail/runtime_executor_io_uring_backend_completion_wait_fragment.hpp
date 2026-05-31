#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_completion_wait_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void complete_io_uring_poll_wait(
            IoUringOperation* operation,
            int result) noexcept {
            IoWaitRegistration* registration = operation->wait_registration;
            if (registration == nullptr || operation->task == nullptr || operation->result == nullptr) {
                untrack_io_uring_operation(operation);
                destroy_io_uring_operation(operation);
                return;
            }

            const int fd = registration->fd;
            auto it = io_waits_.find(fd);
            if (it != io_waits_.end() && it->second == registration) {
                io_waits_.erase(it);
            }

            registration->result->fd = fd;
            registration->result->result = result;
            if (operation->cancel_requested) {
                registration->result->events = io_error;
                registration->result->error = ECANCELED;
                registration->result->result = -ECANCELED;
            } else if (result < 0) {
                registration->result->events = io_error;
                registration->result->error = -result;
            } else {
                registration->result->events = io_events_from_poll(static_cast<std::uint32_t>(result));
                registration->result->error = 0;
            }

            enqueue_pending_blocking(index_, registration->task);
            registration->poll_operation = nullptr;
            operation->wait_registration = nullptr;
            untrack_io_uring_operation(operation);
            destroy_io_uring_operation(operation);
            io_wait_pool_.destroy(registration);
        }
#endif
