#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_timeout_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
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

        void clear_kqueue_timeouts() noexcept {
            KqueueTimeoutRegistration* registration = io_kqueue_timeouts_;
            while (registration != nullptr) {
                KqueueTimeoutRegistration* next = registration->next;
                if (registration->result != nullptr &&
                    registration->result->completion_token == registration) {
                    registration->result->completion_token = nullptr;
                }
                io_kqueue_timeout_pool_.destroy(registration);
                registration = next;
            }
            io_kqueue_timeouts_ = nullptr;
            io_kqueue_timeout_count_ = 0;
        }

        void track_kqueue_timeout(KqueueTimeoutRegistration* registration) noexcept {
            registration->prev = nullptr;
            registration->next = io_kqueue_timeouts_;
            if (io_kqueue_timeouts_ != nullptr) {
                io_kqueue_timeouts_->prev = registration;
            }
            io_kqueue_timeouts_ = registration;
            ++io_kqueue_timeout_count_;
        }

        void untrack_kqueue_timeout(KqueueTimeoutRegistration* registration) noexcept {
            if (registration->prev != nullptr) {
                registration->prev->next = registration->next;
            } else if (io_kqueue_timeouts_ == registration) {
                io_kqueue_timeouts_ = registration->next;
            }
            if (registration->next != nullptr) {
                registration->next->prev = registration->prev;
            }
            registration->prev = nullptr;
            registration->next = nullptr;
            if (io_kqueue_timeout_count_ != 0U) {
                --io_kqueue_timeout_count_;
            }
        }

        [[nodiscard]] uintptr_t next_kqueue_timeout_ident() noexcept {
            uintptr_t ident = io_kqueue_next_timeout_ident_++;
            if (ident <= kqueue_wake_ident) {
                ident = kqueue_wake_ident + 1U;
                io_kqueue_next_timeout_ident_ = ident + 1U;
            }
            return ident;
        }

        [[nodiscard]] static intptr_t kqueue_timeout_data(
            std::chrono::nanoseconds timeout) noexcept {
#if defined(NOTE_NSECONDS)
            return clamp_kqueue_timer_value(timeout.count());
#elif defined(NOTE_USECONDS)
            const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                timeout + std::chrono::nanoseconds{999});
            return clamp_kqueue_timer_value(us.count());
#else
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                timeout + std::chrono::nanoseconds{999999});
            return clamp_kqueue_timer_value(ms.count() == 0 ? 1 : ms.count());
#endif
        }

        [[nodiscard]] static std::uint32_t kqueue_timeout_unit_flags() noexcept {
#if defined(NOTE_NSECONDS)
            return NOTE_NSECONDS;
#elif defined(NOTE_USECONDS)
            return NOTE_USECONDS;
#else
            return 0;
#endif
        }

        [[nodiscard]] static intptr_t clamp_kqueue_timer_value(
            std::int64_t value) noexcept {
            constexpr auto max_value = static_cast<std::uint64_t>(
                std::numeric_limits<intptr_t>::max());
            if (value <= 0) {
                return 1;
            }
            const auto unsigned_value = static_cast<std::uint64_t>(value);
            if (unsigned_value > max_value) {
                return std::numeric_limits<intptr_t>::max();
            }
            return static_cast<intptr_t>(value);
        }
