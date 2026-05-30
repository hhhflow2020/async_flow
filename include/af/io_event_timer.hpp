#pragma once

#include "af/io_datagram.hpp"

namespace af {

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_eventfd(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint64_t* value,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (value == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    detail::clear_waiting(state);
    for (;;) {
        std::uint64_t counter = 0;
        const ssize_t n = ::read(fd, &counter, sizeof(counter));
        if (n == static_cast<ssize_t>(sizeof(counter))) {
            *value = counter;
            return IoStatus::ready(sizeof(counter));
        }
        if (n >= 0) {
            return IoStatus::failed(EIO);
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_timerfd(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint64_t* expirations,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (expirations == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    detail::clear_waiting(state);
    for (;;) {
        std::uint64_t value = 0;
        const ssize_t n = ::read(fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            *expirations = value;
            return IoStatus::ready(sizeof(value));
        }
        if (n >= 0) {
            return IoStatus::failed(EIO);
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

} // namespace af
