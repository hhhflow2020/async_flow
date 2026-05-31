#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_external_post_gate_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    [[nodiscard]] static bool try_enter_post(std::uint16_t target) noexcept {
        const RuntimeStatus status = status_.load(std::memory_order_acquire);
        if (current_thread_index_ < thread_count) {
            if (status == RuntimeStatus::Running) {
                return true;
            }

            if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
                return status == RuntimeStatus::Stopping;
            }

            return false;
        }

        if (status == RuntimeStatus::Running) {
            active_external_posts_[target].value.fetch_add(1, std::memory_order_acq_rel);
            if (status_.load(std::memory_order_acquire) == RuntimeStatus::Running) {
                return true;
            }

            leave_post(target);
            return false;
        }

        return false;
    }

    static void leave_post(std::uint16_t target) noexcept {
        if (current_thread_index_ < thread_count) {
            return;
        }

        AF_ASSERT(target < thread_count);
        if (active_external_posts_[target].value.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            active_external_posts_[target].value.notify_all();
        }
    }

    static void wait_for_external_posts() noexcept {
        for (auto& counter : active_external_posts_) {
            for (;;) {
                const std::uint32_t count = counter.value.load(std::memory_order_acquire);
                if (count == 0) {
                    break;
                }
                counter.value.wait(count, std::memory_order_acquire);
            }
        }
    }
