#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_wait_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_native_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            bool prefer_rearm) noexcept {
            static_cast<void>(prefer_rearm);
            const bool unsupported_events = (events & (io_readable | io_writable)) == 0U;
            if (io_kqueue_fd_ < 0 || fd < 0 || events == 0U || unsupported_events ||
                io_waits_.find(fd) != io_waits_.end()) {
                result->fd = fd;
                result->events = io_error;
                if (fd < 0) {
                    result->error = EBADF;
                } else if (events == 0U || unsupported_events) {
                    result->error = EINVAL;
                } else if (io_kqueue_fd_ < 0) {
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

            std::array<struct kevent, 2> changes;
            int change_count = fill_kqueue_changes(fd, events, registration, changes);
            if (::kevent(io_kqueue_fd_, changes.data(), change_count, nullptr, 0, nullptr) != 0) {
                const int error = errno == 0 ? EIO : errno;
                io_waits_.erase(fd);
                io_wait_pool_.destroy(registration);
                result->fd = fd;
                result->events = io_error;
                result->error = error;
                return false;
            }

            *result = IoResult{fd, 0, 0};
            return true;
        }

        [[nodiscard]] bool cancel_native_io_wait(IoOpState& state) noexcept {
            if (io_kqueue_fd_ < 0) {
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
            remove_kqueue_filters(*registration);
            io_waits_.erase(it);

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

        [[nodiscard]] static int fill_kqueue_changes(
            int fd,
            std::uint32_t events,
            IoWaitRegistration* registration,
            std::array<struct kevent, 2>& changes) noexcept {
            int count = 0;
            if ((events & io_readable) != 0U) {
                EV_SET(
                    &changes[static_cast<std::size_t>(count++)],
                    static_cast<uintptr_t>(fd),
                    EVFILT_READ,
                    EV_ADD | EV_ONESHOT,
                    0,
                    0,
                    registration);
            }
            if ((events & io_writable) != 0U) {
                EV_SET(
                    &changes[static_cast<std::size_t>(count++)],
                    static_cast<uintptr_t>(fd),
                    EVFILT_WRITE,
                    EV_ADD | EV_ONESHOT,
                    0,
                    0,
                    registration);
            }
            return count;
        }

        void remove_kqueue_filters(const IoWaitRegistration& registration) noexcept {
            std::array<struct kevent, 2> changes;
            int count = 0;
            if ((registration.events & io_readable) != 0U) {
                EV_SET(
                    &changes[static_cast<std::size_t>(count++)],
                    static_cast<uintptr_t>(registration.fd),
                    EVFILT_READ,
                    EV_DELETE,
                    0,
                    0,
                    nullptr);
            }
            if ((registration.events & io_writable) != 0U) {
                EV_SET(
                    &changes[static_cast<std::size_t>(count++)],
                    static_cast<uintptr_t>(registration.fd),
                    EVFILT_WRITE,
                    EV_DELETE,
                    0,
                    0,
                    nullptr);
            }
            if (count != 0) {
                static_cast<void>(::kevent(io_kqueue_fd_, changes.data(), count, nullptr, 0, nullptr));
            }
        }
