#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void notify_force() noexcept {
            if (notify_native_io_backend()) {
                return;
            }
            wake_epoch_.fetch_add(1, std::memory_order_release);
            wake_epoch_.notify_one();
        }

        void run_loop() noexcept {
            current_thread_index_ = index_;

            for (;;) {
                bool did_work = false;
                while (Task* task = pop_one()) {
                    did_work = true;
                    execute(task);
                }

#if defined(__linux__)
                if (flush_io_uring_submissions_or_fail()) {
                    did_work = true;
                }
#endif
                flush_native_io_backend_deferred_deletes();

                if (stop_requested_.load(std::memory_order_acquire)) {
                    if (!did_work) {
                        break;
                    }
                    continue;
                }

                if (poll_io(0)) {
                    continue;
                }

                const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
                sleeping_.store(true, std::memory_order_release);
                if (stop_requested_.load(std::memory_order_acquire)) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    continue;
                }

                if (Task* task = pop_one()) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    execute(task);
                } else {
                    if (io_thread() && io_backend_available()) {
                        static_cast<void>(poll_io(-1));
                    } else if (wake_epoch_.load(std::memory_order_acquire) == observed) {
                        wake_epoch_.wait(observed, std::memory_order_acquire);
                    }
                    sleeping_.store(false, std::memory_order_relaxed);
                }
            }

            current_thread_index_ = invalid_thread_index;
        }

        void init_io_backend() noexcept {
            if (!io_thread() || native_io_backend_available()) {
                return;
            }
            if (!init_native_io_backend()) {
                return;
            }
#if defined(__linux__)
            if (io_uring_thread()) {
                init_io_uring_backend();
            }
#endif
        }

        void close_io_backend() noexcept {
#if defined(__linux__)
            close_io_uring_backend();
#endif
            close_native_io_backend();
        }

        [[nodiscard]] bool poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
            bool did_work = poll_io_uring_completions();
#else
            bool did_work = false;
#endif
            return poll_native_io(timeout_ms, did_work);
        }
