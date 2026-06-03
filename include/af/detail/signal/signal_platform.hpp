#pragma once

#include <cerrno>
#include <chrono>
#include <initializer_list>

#include <pthread.h>
#include <signal.h>
#include <time.h>

namespace af::detail {

[[nodiscard]] inline timespec
signal_timespec_from_nanoseconds(std::chrono::nanoseconds timeout) noexcept {
    if (timeout.count() < 0) {
        timeout = std::chrono::nanoseconds(0);
    }
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    timeout -= seconds;
    return timespec{static_cast<time_t>(seconds.count()), static_cast<long>(timeout.count())};
}

[[nodiscard]] inline std::chrono::nanoseconds
signal_remaining_timeout(std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        return std::chrono::nanoseconds(0);
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
}

[[nodiscard]] inline bool signal_pending_in_set(const sigset_t &set, int &signal,
                                                int &error) noexcept {
    sigset_t pending{};
    if (::sigpending(&pending) != 0) {
        error = errno == 0 ? EINVAL : errno;
        return false;
    }

#if defined(NSIG)
    constexpr int max_signal = NSIG;
#else
    constexpr int max_signal = 128;
#endif
    for (int candidate = 1; candidate < max_signal; ++candidate) {
        const int in_set = sigismember(&set, candidate);
        if (in_set < 0) {
            error = errno == 0 ? EINVAL : errno;
            return false;
        }
        if (in_set == 0) {
            continue;
        }
        const int is_pending = sigismember(&pending, candidate);
        if (is_pending == 1) {
            signal = candidate;
            error = 0;
            return true;
        }
        if (is_pending < 0) {
            error = errno == 0 ? EINVAL : errno;
            return false;
        }
    }

    error = 0;
    return false;
}

[[nodiscard]] inline bool signal_can_be_waited(int signal) noexcept {
#if defined(SIGKILL)
    if (signal == SIGKILL) {
        return false;
    }
#endif
#if defined(SIGSTOP)
    if (signal == SIGSTOP) {
        return false;
    }
#endif
    return true;
}

class SignalSetImpl {
public:
    explicit SignalSetImpl(std::initializer_list<int> signals) noexcept {
        if (signals.size() == 0U) {
            error_ = EINVAL;
            return;
        }
        if (sigemptyset(&set_) != 0) {
            error_ = errno == 0 ? EINVAL : errno;
            return;
        }
        for (const int signal : signals) {
            if (!signal_can_be_waited(signal)) {
                error_ = EINVAL;
                return;
            }
            if (sigaddset(&set_, signal) != 0) {
                error_ = errno == 0 ? EINVAL : errno;
                return;
            }
        }

        const int error = ::pthread_sigmask(SIG_BLOCK, &set_, &previous_set_);
        if (error != 0) {
            error_ = error;
            return;
        }
        blocked_ = true;
    }

    SignalSetImpl(const SignalSetImpl &) = delete;
    SignalSetImpl &operator=(const SignalSetImpl &) = delete;

    ~SignalSetImpl() {
        if (blocked_) {
            static_cast<void>(::pthread_sigmask(SIG_SETMASK, &previous_set_, nullptr));
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return error_ == 0;
    }

    [[nodiscard]] int error() const noexcept {
        return error_;
    }

    [[nodiscard]] SignalWaitResult wait_for(std::chrono::nanoseconds timeout) noexcept {
        if (!valid()) {
            return SignalWaitResult{0, error_ == 0 ? EINVAL : error_};
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
#if defined(__linux__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
        siginfo_t info{};
        for (;;) {
            auto remaining = signal_remaining_timeout(deadline);
            timespec timespec_timeout = signal_timespec_from_nanoseconds(remaining);
            const int signal = ::sigtimedwait(&set_, &info, &timespec_timeout);
            if (signal >= 0) {
                return SignalWaitResult{signal, 0};
            }
            if (errno != EINTR) {
                const int error = errno == 0 ? EINVAL : errno;
                return SignalWaitResult{0, error};
            }
            if (remaining.count() == 0) {
                return SignalWaitResult{0, EAGAIN};
            }
        }
#else
        for (;;) {
            int signal = 0;
            int error = 0;
            if (signal_pending_in_set(set_, signal, error)) {
                return wait();
            }
            if (error != 0) {
                return SignalWaitResult{0, error};
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return SignalWaitResult{0, EAGAIN};
            }

            const auto remaining = signal_remaining_timeout(deadline);
            const auto sleep_ns =
                remaining < std::chrono::milliseconds(1)
                    ? std::chrono::duration_cast<std::chrono::nanoseconds>(remaining)
                    : std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::milliseconds(1));
            timespec sleep_time = signal_timespec_from_nanoseconds(sleep_ns);
            while (::nanosleep(&sleep_time, &sleep_time) != 0 && errno == EINTR) {
            }
        }
#endif
    }

    [[nodiscard]] SignalWaitResult wait() noexcept {
        if (!valid()) {
            return SignalWaitResult{0, error_ == 0 ? EINVAL : error_};
        }

        int signal = 0;
        int error = 0;
        do {
            error = ::sigwait(&set_, &signal);
        } while (error == EINTR);

        if (error != 0) {
            return SignalWaitResult{0, error};
        }
        return SignalWaitResult{signal, 0};
    }

private:
    int error_{0};
    sigset_t set_{};
    sigset_t previous_set_{};
    bool blocked_{false};
};

[[nodiscard]] inline bool ignore_process_signal_impl(int signal) noexcept {
    struct sigaction action{};
    action.sa_handler = SIG_IGN;
    if (sigemptyset(&action.sa_mask) != 0) {
        return false;
    }
    return ::sigaction(signal, &action, nullptr) == 0;
}

} // namespace af::detail
