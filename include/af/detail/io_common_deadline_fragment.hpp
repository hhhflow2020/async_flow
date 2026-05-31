#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_deadline_fragment.hpp is an io_common implementation fragment"
#endif

struct IoDeadline {
    std::chrono::nanoseconds delay{0};
    UniqueFd timer{};
    IoOpState wait{};
    std::uint64_t expirations{0};
    bool armed{false};
    bool cancel_pending{false};
    bool ring_timeout{false};
    bool timeout_cancel_pending{false};

    void set_after(std::chrono::nanoseconds timeout) noexcept {
        delay = timeout;
        reset_runtime();
    }

    void reset_runtime() noexcept {
        wait.reset();
        expirations = 0;
        armed = false;
        cancel_pending = false;
        ring_timeout = false;
        timeout_cancel_pending = false;
    }

    void reset() noexcept {
        reset_runtime();
        timer.reset();
        delay = std::chrono::nanoseconds{0};
    }

    [[nodiscard]] bool configured() const noexcept {
        return delay.count() > 0;
    }
};
