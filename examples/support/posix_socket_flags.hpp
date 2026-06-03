#pragma once

#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace asyncflow_examples {

[[nodiscard]] inline int socket_type_with_flags(int base) noexcept {
#if defined(SOCK_NONBLOCK)
    base |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    base |= SOCK_CLOEXEC;
#endif
    return base;
}

[[nodiscard]] inline bool apply_socket_flags(int fd) noexcept {
#if !defined(SOCK_NONBLOCK)
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return false;
    }
#endif

#if !defined(SOCK_CLOEXEC)
    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return false;
    }
#endif
    return true;
}

[[nodiscard]] inline bool apply_socket_flags(int fd, int &error) noexcept {
    error = 0;
    if (!apply_socket_flags(fd)) {
        error = errno == 0 ? EIO : errno;
        return false;
    }
    return true;
}

namespace detail {

[[nodiscard]] inline bool set_socket_flags(int fd) noexcept {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return false;
    }

    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return false;
    }
    return true;
}

inline void close_socket_pair_after_failure(int fds[2], int error) noexcept {
    for (int index = 0; index < 2; ++index) {
        if (fds[index] >= 0) {
            static_cast<void>(::close(fds[index]));
            fds[index] = -1;
        }
    }
    errno = error;
}

} // namespace detail

[[nodiscard]] inline bool socket_pair_with_flags(int domain, int type, int protocol,
                                                 int fds[2]) noexcept {
    fds[0] = -1;
    fds[1] = -1;

    const int flagged_type = socket_type_with_flags(type);
    if (::socketpair(domain, flagged_type, protocol, fds) == 0) {
        if (apply_socket_flags(fds[0]) && apply_socket_flags(fds[1])) {
            return true;
        }
        detail::close_socket_pair_after_failure(fds, errno == 0 ? EIO : errno);
        return false;
    }

    if (flagged_type == type || ::socketpair(domain, type, protocol, fds) != 0) {
        return false;
    }

    if (detail::set_socket_flags(fds[0]) && detail::set_socket_flags(fds[1])) {
        return true;
    }
    detail::close_socket_pair_after_failure(fds, errno == 0 ? EIO : errno);
    return false;
}

} // namespace asyncflow_examples
