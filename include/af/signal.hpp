#pragma once

#include <chrono>
#include <csignal>
#include <initializer_list>

namespace af {

struct signal_wait_result {
    int signal{0};
    int error{0};

    [[nodiscard]] bool ok() const noexcept {
        return error == 0;
    }
};

} // namespace af

#include "af/detail/signal/signal_platform.hpp"

namespace af {

class signal_set {
public:
    explicit signal_set(std::initializer_list<int> signals) noexcept : impl_(signals) {}

    signal_set(const signal_set &) = delete;
    signal_set &operator=(const signal_set &) = delete;

    ~signal_set() = default;

    [[nodiscard]] bool valid() const noexcept {
        return impl_.valid();
    }

    [[nodiscard]] int error() const noexcept {
        return impl_.error();
    }

    [[nodiscard]] signal_wait_result try_wait() noexcept {
        return wait_for(std::chrono::nanoseconds(0));
    }

    template <typename Rep, typename Period>
    [[nodiscard]] signal_wait_result wait_for(std::chrono::duration<Rep, Period> timeout) noexcept {
        return impl_.wait_for(std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] signal_wait_result
    wait_until(std::chrono::time_point<Clock, Duration> deadline) noexcept {
        return wait_for(deadline - Clock::now());
    }

    [[nodiscard]] signal_wait_result wait() noexcept {
        return impl_.wait();
    }

private:
    detail::signal_set_impl impl_;
};

[[nodiscard]] inline signal_set make_termination_signal_set() noexcept {
    return signal_set({SIGINT, SIGTERM});
}

[[nodiscard]] inline bool ignore_process_signal(int signal) noexcept {
    return detail::ignore_process_signal_impl(signal);
}

using SignalWaitResult = signal_wait_result;
using SignalSet = signal_set;

} // namespace af
