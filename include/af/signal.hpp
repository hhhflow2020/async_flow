#pragma once

#include <chrono>
#include <csignal>
#include <initializer_list>

namespace af {

struct SignalWaitResult {
    int signal{0};
    int error{0};

    [[nodiscard]] bool ok() const noexcept {
        return error == 0;
    }
};

} // namespace af

#include "af/detail/signal/signal_platform.hpp"

namespace af {

class SignalSet {
public:
    explicit SignalSet(std::initializer_list<int> signals) noexcept : impl_(signals) {}

    SignalSet(const SignalSet &) = delete;
    SignalSet &operator=(const SignalSet &) = delete;

    ~SignalSet() = default;

    [[nodiscard]] bool valid() const noexcept {
        return impl_.valid();
    }

    [[nodiscard]] int error() const noexcept {
        return impl_.error();
    }

    [[nodiscard]] SignalWaitResult try_wait() noexcept {
        return wait_for(std::chrono::nanoseconds(0));
    }

    template <typename Rep, typename Period>
    [[nodiscard]] SignalWaitResult wait_for(std::chrono::duration<Rep, Period> timeout) noexcept {
        return impl_.wait_for(std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] SignalWaitResult
    wait_until(std::chrono::time_point<Clock, Duration> deadline) noexcept {
        return wait_for(deadline - Clock::now());
    }

    [[nodiscard]] SignalWaitResult wait() noexcept {
        return impl_.wait();
    }

private:
    detail::SignalSetImpl impl_;
};

[[nodiscard]] inline SignalSet make_termination_signal_set() noexcept {
    return SignalSet({SIGINT, SIGTERM});
}

[[nodiscard]] inline bool ignore_process_signal(int signal) noexcept {
    return detail::ignore_process_signal_impl(signal);
}

} // namespace af
