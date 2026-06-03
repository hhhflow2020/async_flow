#pragma once

#if defined(__linux__)
[[nodiscard]] inline UniqueFd
make_eventfd(unsigned int init_value = 0, int flags = detail::io_default_eventfd_flags()) noexcept {
    return UniqueFd(::eventfd(init_value, flags));
}

[[nodiscard]] inline bool write_eventfd(int fd, std::uint64_t value, int &error) noexcept {
    error = 0;
    if (fd < 0) {
        error = EBADF;
        return false;
    }
    if (value == std::numeric_limits<std::uint64_t>::max()) {
        error = EINVAL;
        return false;
    }

    for (;;) {
        const ssize_t n = ::write(fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            return true;
        }
        if (n >= 0) {
            error = EIO;
            return false;
        }

        error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] inline UniqueFd
make_timerfd(clockid_t clock_id = CLOCK_MONOTONIC,
             int flags = detail::io_default_timerfd_flags()) noexcept {
    return UniqueFd(::timerfd_create(clock_id, flags));
}

[[nodiscard]] inline bool arm_timerfd(int fd, std::chrono::nanoseconds initial,
                                      std::chrono::nanoseconds interval, int &error) noexcept {
    error = 0;
    if (fd < 0) {
        error = EBADF;
        return false;
    }
    if (initial.count() < 0 || interval.count() < 0) {
        error = EINVAL;
        return false;
    }

    itimerspec spec{};
    if (initial.count() != 0) {
        spec.it_value = detail::io_timespec_from_duration(initial);
    }
    if (interval.count() != 0) {
        spec.it_interval = detail::io_timespec_from_duration(interval);
    }
    if (::timerfd_settime(fd, 0, &spec, nullptr) != 0) {
        error = errno == 0 ? EIO : errno;
        return false;
    }
    return true;
}

[[nodiscard]] inline bool arm_timerfd_after(int fd, std::chrono::nanoseconds delay,
                                            int &error) noexcept {
    if (delay.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, delay, std::chrono::nanoseconds{0}, error);
}

[[nodiscard]] inline bool arm_timerfd_every(int fd, std::chrono::nanoseconds interval,
                                            int &error) noexcept {
    if (interval.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, interval, interval, error);
}

[[nodiscard]] inline bool disarm_timerfd(int fd, int &error) noexcept {
    return arm_timerfd(fd, std::chrono::nanoseconds{0}, std::chrono::nanoseconds{0}, error);
}
#endif

#if !defined(__linux__)
[[nodiscard]] inline UniqueFd make_eventfd(unsigned int init_value = 0, int flags = 0) noexcept {
    static_cast<void>(init_value);
    static_cast<void>(flags);
    return UniqueFd();
}

[[nodiscard]] inline bool write_eventfd(int fd, std::uint64_t value, int &error) noexcept {
    static_cast<void>(fd);
    static_cast<void>(value);
    error = ENOSYS;
    return false;
}

[[nodiscard]] inline UniqueFd make_timerfd() noexcept {
    return UniqueFd();
}

[[nodiscard]] inline bool arm_timerfd(int fd, std::chrono::nanoseconds initial,
                                      std::chrono::nanoseconds interval, int &error) noexcept {
    static_cast<void>(fd);
    static_cast<void>(initial);
    static_cast<void>(interval);
    error = ENOSYS;
    return false;
}

[[nodiscard]] inline bool arm_timerfd_after(int fd, std::chrono::nanoseconds delay,
                                            int &error) noexcept {
    static_cast<void>(fd);
    static_cast<void>(delay);
    error = ENOSYS;
    return false;
}

[[nodiscard]] inline bool arm_timerfd_every(int fd, std::chrono::nanoseconds interval,
                                            int &error) noexcept {
    static_cast<void>(fd);
    static_cast<void>(interval);
    error = ENOSYS;
    return false;
}

[[nodiscard]] inline bool disarm_timerfd(int fd, int &error) noexcept {
    static_cast<void>(fd);
    error = ENOSYS;
    return false;
}
#endif
