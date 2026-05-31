#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if AF_DETAIL_HAS_KQUEUE
        static constexpr uintptr_t kqueue_wake_ident = 1;

        [[nodiscard]] bool native_io_backend_available() const noexcept {
            return io_kqueue_fd_ >= 0;
        }

        [[nodiscard]] bool notify_native_io_backend() noexcept {
            if (io_kqueue_fd_ < 0) {
                return false;
            }
            bool expected = false;
            if (!io_wake_pending_.compare_exchange_strong(
                    expected,
                    true,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return true;
            }

            struct kevent event {};
            EV_SET(&event, kqueue_wake_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
                io_wake_pending_.store(false, std::memory_order_release);
                return false;
            }
            return true;
        }

        [[nodiscard]] bool init_native_io_backend() noexcept {
            if (!native_io_thread()) {
                return false;
            }
            if (io_kqueue_fd_ >= 0) {
                return true;
            }
            reserve_native_io_wait_storage();

            io_kqueue_fd_ = ::kqueue();
            if (io_kqueue_fd_ < 0) {
                return false;
            }

            struct kevent event {};
            EV_SET(&event, kqueue_wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
            if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
                close_native_io_backend();
                return false;
            }
            return true;
        }

        void close_native_io_backend() noexcept {
            clear_io_waits();
            if (io_kqueue_fd_ >= 0) {
                ::close(io_kqueue_fd_);
                io_kqueue_fd_ = -1;
            }
            io_wake_pending_.store(false, std::memory_order_relaxed);
        }

        [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
            if (io_kqueue_fd_ < 0) {
                return did_work;
            }
            if (timeout_ms == 0 && io_waits_.empty() &&
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
                auto* registration = static_cast<IoWaitRegistration*>(event.udata);
                if (registration == nullptr || event.filter == EVFILT_USER) {
                    io_wake_pending_.store(false, std::memory_order_release);
                    did_work = true;
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

        void flush_native_io_backend_deferred_deletes() noexcept {}

        void clear_io_waits() noexcept {
            for (auto& entry : io_waits_) {
                io_wait_pool_.destroy(entry.second);
            }
            io_waits_.clear();
        }

        void reserve_native_io_wait_storage() noexcept {
            try {
                if constexpr (io_wait_reserve != 0U) {
                    io_waits_.reserve(io_wait_reserve);
                }
            } catch (...) {
            }
        }

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

        [[nodiscard]] static std::uint32_t io_events_from_kqueue(
            const struct kevent& event) noexcept {
            if ((event.flags & EV_ERROR) != 0) {
                return io_error;
            }

            std::uint32_t result = 0;
            if (event.filter == EVFILT_READ) {
                result |= io_readable;
            } else if (event.filter == EVFILT_WRITE) {
                result |= io_writable;
            }
            if ((event.flags & EV_EOF) != 0) {
                result |= io_hangup;
            }
            return result;
        }

        [[nodiscard]] static int io_error_from_kqueue(
            const struct kevent& event) noexcept {
            if ((event.flags & EV_ERROR) == 0) {
                return 0;
            }
            return event.data == 0 ? EIO : static_cast<int>(event.data);
        }
#endif
