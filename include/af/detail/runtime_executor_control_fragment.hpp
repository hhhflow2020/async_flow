#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_control_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        explicit Executor(std::uint16_t index)
            : index_(index),
              kind_(thread_kind(thread_from_index(index))),
              local_queue_(detail::next_power_of_two(spsc_queue_capacity < 2 ? 2 : spsc_queue_capacity)) {}
        Executor(const Executor&) = delete;
        Executor& operator=(const Executor&) = delete;

        ~Executor() {
            close_io_backend();
        }

        void start() {
            init_io_backend();
            worker_ = std::thread([this] {
                run_loop();
            });
        }

        void request_stop() noexcept {
            stop_requested_.store(true, std::memory_order_release);
            notify_force();
        }

        void join() {
            if (worker_.joinable()) {
                worker_.join();
            }
        }

        void notify() noexcept {
            if (!sleeping_.load(std::memory_order_acquire)) {
                return;
            }

            bool expected = true;
            if (sleeping_.compare_exchange_strong(
                    expected,
                    false,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                notify_force();
            }
        }
