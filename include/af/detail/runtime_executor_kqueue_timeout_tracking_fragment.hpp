#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_timeout_tracking_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

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
