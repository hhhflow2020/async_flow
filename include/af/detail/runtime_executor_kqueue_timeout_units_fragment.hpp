#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_timeout_units_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

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
