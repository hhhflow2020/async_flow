#pragma once

#include "af/io_types.hpp"

namespace af {

namespace detail {

inline constexpr std::uint64_t io_current_offset = std::numeric_limits<std::uint64_t>::max();

[[nodiscard]] inline bool io_would_block(int error) noexcept {
    return error == EAGAIN
#if EWOULDBLOCK != EAGAIN
        || error == EWOULDBLOCK
#endif
        ;
}

[[nodiscard]] inline int io_no_signal_flag() noexcept {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

[[nodiscard]] inline int io_default_accept_flags() noexcept {
    int flags = 0;
#if defined(SOCK_NONBLOCK)
    flags |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    flags |= SOCK_CLOEXEC;
#endif
    return flags;
}

#if defined(__linux__)
[[nodiscard]] inline int io_default_eventfd_flags() noexcept {
    int flags = 0;
#if defined(EFD_NONBLOCK)
    flags |= EFD_NONBLOCK;
#endif
#if defined(EFD_CLOEXEC)
    flags |= EFD_CLOEXEC;
#endif
    return flags;
}

[[nodiscard]] inline int io_default_timerfd_flags() noexcept {
    int flags = 0;
#if defined(TFD_NONBLOCK)
    flags |= TFD_NONBLOCK;
#endif
#if defined(TFD_CLOEXEC)
    flags |= TFD_CLOEXEC;
#endif
    return flags;
}

[[nodiscard]] inline timespec io_timespec_from_duration(
    std::chrono::nanoseconds duration) noexcept {
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
    duration -= seconds;
    return timespec{
        static_cast<time_t>(seconds.count()),
        static_cast<long>(duration.count())};
}

struct IoUringRecvmsgOut {
    std::uint32_t namelen{0};
    std::uint32_t controllen{0};
    std::uint32_t payloadlen{0};
    std::uint32_t flags{0};
};
#endif

[[nodiscard]] inline bool io_connect_in_progress(int error) noexcept {
    return error == EINPROGRESS || error == EALREADY || error == EINTR ||
           error == EAGAIN
#if EWOULDBLOCK != EAGAIN
        || error == EWOULDBLOCK
#endif
        ;
}

[[nodiscard]] inline int io_socket_connect_error(int fd) noexcept {
    int error = 0;
    socklen_t error_size = sizeof(error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
        return errno == 0 ? EIO : errno;
    }
    return error;
}

[[nodiscard]] inline bool io_apply_accepted_flags(int fd, int flags, int& error) noexcept {
    error = 0;
#if defined(SOCK_NONBLOCK)
    if ((flags & SOCK_NONBLOCK) != 0) {
        const int current = ::fcntl(fd, F_GETFL, 0);
        if (current < 0 || ::fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
    }
#else
    static_cast<void>(flags);
#endif
#if defined(SOCK_CLOEXEC)
    if ((flags & SOCK_CLOEXEC) != 0) {
        const int current = ::fcntl(fd, F_GETFD, 0);
        if (current < 0 || ::fcntl(fd, F_SETFD, current | FD_CLOEXEC) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
    }
#endif
    return true;
}

#if defined(__linux__)
[[nodiscard]] inline bool io_fd_can_wait(int fd) noexcept {
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
        return false;
    }
    return S_ISSOCK(status.st_mode) || S_ISFIFO(status.st_mode) || S_ISCHR(status.st_mode);
}

[[nodiscard]] inline bool io_poll_ready(int fd, short events) noexcept {
    pollfd descriptor{fd, events, 0};
    for (;;) {
        const int ready = ::poll(&descriptor, 1, 0);
        if (ready > 0) {
            return (descriptor.revents & (events | POLLERR | POLLHUP | POLLNVAL)) != 0;
        }
        if (ready == 0) {
            return false;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

#endif

template <typename TaskT>
[[nodiscard]] inline bool io_on_target_thread(typename TaskT::Thread thread) noexcept {
    return TaskT::Runtime::is_runtime_thread() &&
           TaskT::Runtime::current_thread_index() == TaskT::Runtime::thread_index(thread);
}

template <typename TaskT, typename NameFn>
[[nodiscard]] IoStatus io_socket_name(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size,
    NameFn name_fn) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (address == nullptr || address_size == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (!io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (name_fn(fd, address, address_size) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus arm_io_wait(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint32_t events,
    IoOpState& state) noexcept {
    const bool prefer_rearm =
        state.readiness_rearm_hint && state.readiness_fd == fd;
    state.wait = IoResult{fd, 0, 0};
    if (TaskT::Runtime::io_wait(thread, fd, events, &task, &state.wait, prefer_rearm)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Readiness;
        state.readiness_rearm_hint = true;
        state.readiness_fd = fd;
        return IoStatus::make_pending();
    }

    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    if (state.wait.error == EBADF || state.wait.error == ENOENT || state.wait.error == ENOSYS) {
        state.readiness_rearm_hint = false;
        state.readiness_fd = -1;
    }
    return IoStatus::failed(state.wait.error == 0 ? EINVAL : state.wait.error);
}

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus arm_splice_wait(
    TaskT& task,
    typename TaskT::Thread thread,
    int in_fd,
    int out_fd,
    IoOpState& state) noexcept {
    const bool out_waitable = io_fd_can_wait(out_fd);
    const bool in_waitable = io_fd_can_wait(in_fd);
    if (out_waitable && !io_poll_ready(out_fd, POLLOUT)) {
        return arm_io_wait(task, thread, out_fd, io_writable, state);
    }
    if (in_waitable && !io_poll_ready(in_fd, POLLIN)) {
        return arm_io_wait(task, thread, in_fd, io_readable, state);
    }
    if (out_waitable) {
        return arm_io_wait(task, thread, out_fd, io_writable, state);
    }
    if (in_waitable) {
        return arm_io_wait(task, thread, in_fd, io_readable, state);
    }
    return IoStatus::failed(EAGAIN);
}
#endif

[[nodiscard]] inline bool waiting_for_completion(const IoOpState& state) noexcept {
    return state.waiting && state.wait_kind == IoWaitKind::Completion;
}

inline void clear_waiting(IoOpState& state) noexcept {
    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    state.wait.completion_token = nullptr;
}

inline void clear_readiness_rearm_hint(IoOpState& state) noexcept {
    state.readiness_rearm_hint = false;
    state.readiness_fd = -1;
}

[[nodiscard]] inline bool cancelled_wait_ready(const IoOpState& state) noexcept {
    return state.waiting && state.wait.error == ECANCELED;
}

[[nodiscard]] inline bool io_wait_result_ready(const IoOpState& state) noexcept {
    return state.waiting &&
        (state.wait.events != 0U || state.wait.error != 0 || state.wait.result != 0);
}

[[nodiscard]] inline IoStatus consume_cancelled_wait(IoOpState& state) noexcept {
    clear_waiting(state);
    clear_readiness_rearm_hint(state);
    return IoStatus::failed(ECANCELED);
}

[[nodiscard]] inline bool uring_submit_error_can_fallback(int error) noexcept {
    return error == ENOSYS || error == EBUSY;
}

[[nodiscard]] inline bool uring_zero_copy_send_error_can_fallback(int error) noexcept {
    return error == ENOSYS || error == EINVAL
#ifdef EOPNOTSUPP
        || error == EOPNOTSUPP
#endif
#ifdef EAFNOSUPPORT
        || error == EAFNOSUPPORT
#endif
        ;
}

#if !defined(_WIN32)
[[nodiscard]] inline int io_max_iov() noexcept {
#if defined(IOV_MAX)
    return IOV_MAX;
#else
    return 1024;
#endif
}

[[nodiscard]] inline bool io_validate_iov(
    const iovec* iov,
    int iov_count,
    std::size_t& total_size,
    int& error) noexcept {
    total_size = 0;
    error = 0;
    if (iov_count < 0 || iov_count > io_max_iov()) {
        error = EINVAL;
        return false;
    }
    if (iov_count == 0) {
        return true;
    }
    if (iov == nullptr) {
        error = EINVAL;
        return false;
    }

    for (int i = 0; i < iov_count; ++i) {
        const std::size_t len = iov[i].iov_len;
        if (len != 0U && iov[i].iov_base == nullptr) {
            error = EINVAL;
            return false;
        }
        if (len > std::numeric_limits<std::size_t>::max() - total_size) {
            error = EOVERFLOW;
            return false;
        }
        total_size += len;
    }
    return true;
}
#endif

[[nodiscard]] inline IoStatus completed_uring_status(
    IoOpState& state,
    bool zero_is_closed = false) noexcept {
    clear_waiting(state);
    if (state.wait.error != 0) {
        return IoStatus::failed(state.wait.error);
    }
    if (state.wait.result < 0) {
        return IoStatus::failed(static_cast<int>(-state.wait.result));
    }
    if (zero_is_closed && state.wait.result == 0) {
        return IoStatus::make_closed();
    }
    return IoStatus::ready(static_cast<std::size_t>(state.wait.result));
}

inline void reset_multishot_completion_wait(IoOpState& state, int fd) noexcept {
    void* const completion_token = state.wait.completion_token;
    state.wait = IoResult{fd, 0, 0, 0};
    state.wait.completion_token = completion_token;
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
}

} // namespace detail

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_recvv_fixed_file_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const iovec* iov,
    int iov_count,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state, true);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_recvmsg_fixed_file_iov(
            thread,
            file_index,
            iov,
            iov_count,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_fixed_file_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const iovec* iov,
    int iov_count,
    IoOpState& state,
    std::uint32_t flags = static_cast<std::uint32_t>(detail::io_no_signal_flag())) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_sendmsg_fixed_file_iov(
            thread,
            file_index,
            iov,
            iov_count,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif

#if defined(__linux__)
[[nodiscard]] inline UniqueFd make_eventfd(
    unsigned int init_value = 0,
    int flags = detail::io_default_eventfd_flags()) noexcept {
    return UniqueFd(::eventfd(init_value, flags));
}

[[nodiscard]] inline bool write_eventfd(
    int fd,
    std::uint64_t value,
    int& error) noexcept {
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

[[nodiscard]] inline UniqueFd make_timerfd(
    clockid_t clock_id = CLOCK_MONOTONIC,
    int flags = detail::io_default_timerfd_flags()) noexcept {
    return UniqueFd(::timerfd_create(clock_id, flags));
}

[[nodiscard]] inline bool arm_timerfd(
    int fd,
    std::chrono::nanoseconds initial,
    std::chrono::nanoseconds interval,
    int& error) noexcept {
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

[[nodiscard]] inline bool arm_timerfd_after(
    int fd,
    std::chrono::nanoseconds delay,
    int& error) noexcept {
    if (delay.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, delay, std::chrono::nanoseconds{0}, error);
}

[[nodiscard]] inline bool arm_timerfd_every(
    int fd,
    std::chrono::nanoseconds interval,
    int& error) noexcept {
    if (interval.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, interval, interval, error);
}

[[nodiscard]] inline bool disarm_timerfd(int fd, int& error) noexcept {
    return arm_timerfd(
        fd,
        std::chrono::nanoseconds{0},
        std::chrono::nanoseconds{0},
        error);
}
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

} // namespace af
