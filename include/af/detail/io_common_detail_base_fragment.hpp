#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_detail_base_fragment.hpp is an io_common implementation fragment"
#endif

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

} // namespace detail
