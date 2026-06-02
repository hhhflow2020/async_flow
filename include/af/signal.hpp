#pragma once

#include <cerrno>
#include <csignal>
#include <initializer_list>

#if !defined(_WIN32)
#include <pthread.h>
#include <signal.h>
#endif

namespace af {

struct SignalWaitResult {
    int signal{0};
    int error{0};

    [[nodiscard]] bool ok() const noexcept {
        return error == 0;
    }
};

namespace detail {

[[nodiscard]] inline constexpr int unsupported_signal_error() noexcept {
#if defined(ENOSYS)
    return ENOSYS;
#else
    return EINVAL;
#endif
}

} // namespace detail

class SignalSet {
public:
    explicit SignalSet(std::initializer_list<int> signals) noexcept {
#if defined(_WIN32)
        static_cast<void>(signals);
        error_ = detail::unsupported_signal_error();
#else
        if (signals.size() == 0U) {
            error_ = EINVAL;
            return;
        }
        if (::sigemptyset(&set_) != 0) {
            error_ = errno == 0 ? EINVAL : errno;
            return;
        }
        for (const int signal : signals) {
            if (::sigaddset(&set_, signal) != 0) {
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
#endif
    }

    SignalSet(const SignalSet &) = delete;
    SignalSet &operator=(const SignalSet &) = delete;

    ~SignalSet() {
#if !defined(_WIN32)
        if (blocked_) {
            static_cast<void>(::pthread_sigmask(SIG_SETMASK, &previous_set_, nullptr));
        }
#endif
    }

    [[nodiscard]] bool valid() const noexcept {
        return error_ == 0;
    }

    [[nodiscard]] int error() const noexcept {
        return error_;
    }

    [[nodiscard]] SignalWaitResult wait() noexcept {
        if (!valid()) {
            return SignalWaitResult{0, error_ == 0 ? EINVAL : error_};
        }

#if defined(_WIN32)
        return SignalWaitResult{0, detail::unsupported_signal_error()};
#else
        int signal = 0;
        int error = 0;
        do {
            error = ::sigwait(&set_, &signal);
        } while (error == EINTR);

        if (error != 0) {
            return SignalWaitResult{0, error};
        }
        return SignalWaitResult{signal, 0};
#endif
    }

private:
    int error_{0};
#if !defined(_WIN32)
    sigset_t set_{};
    sigset_t previous_set_{};
    bool blocked_{false};
#endif
};

[[nodiscard]] inline bool ignore_process_signal(int signal) noexcept {
#if defined(_WIN32)
    static_cast<void>(signal);
    return false;
#else
    struct sigaction action{};
    action.sa_handler = SIG_IGN;
    if (::sigemptyset(&action.sa_mask) != 0) {
        return false;
    }
    return ::sigaction(signal, &action, nullptr) == 0;
#endif
}

} // namespace af
