#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_timeout_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_kqueue_timeout(
            std::chrono::nanoseconds timeout,
            Task* task,
            IoResult* result) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "kqueue timeout submit must run on its IO thread");
            if (result != nullptr) {
                result->completion_token = nullptr;
            }
            if (current_thread_index_ != index_ ||
                task == nullptr ||
                result == nullptr ||
                timeout.count() <= 0) {
                if (result != nullptr) {
                    result->fd = -1;
                    result->events = io_error;
                    result->error = EINVAL;
                    result->result = -EINVAL;
                }
                return false;
            }
            if (io_kqueue_fd_ < 0) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOSYS;
                result->result = -ENOSYS;
                return false;
            }

            KqueueTimeoutRegistration* registration = nullptr;
            try {
                registration = io_kqueue_timeout_pool_.create();
            } catch (...) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOMEM;
                result->result = -ENOMEM;
                return false;
            }

            registration->task = task;
            registration->result = result;
            registration->prev = nullptr;
            registration->next = nullptr;
            registration->ident = next_kqueue_timeout_ident();

            struct kevent event {};
            EV_SET(
                &event,
                registration->ident,
                EVFILT_TIMER,
                EV_ADD | EV_ONESHOT,
                kqueue_timeout_unit_flags(),
                kqueue_timeout_data(timeout),
                registration);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
                const int error = errno == 0 ? EIO : errno;
                io_kqueue_timeout_pool_.destroy(registration);
                result->fd = -1;
                result->events = io_error;
                result->error = error;
                result->result = -error;
                return false;
            }

            track_kqueue_timeout(registration);
            result->fd = -1;
            result->events = 0;
            result->error = 0;
            result->result = 0;
            result->completion_token = registration;
            return true;
        }
