#pragma once

#include <cerrno>

#include "af/net/tcp/tcp_types.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#include <unistd.h>

namespace af::net::detail {

[[nodiscard]] inline bool set_nonblocking(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFL, 0);
    return current >= 0 && ::fcntl(fd, F_SETFL, current | O_NONBLOCK) == 0;
}

[[nodiscard]] inline bool set_cloexec(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFD, 0);
    return current >= 0 && ::fcntl(fd, F_SETFD, current | FD_CLOEXEC) == 0;
}

[[nodiscard]] inline int send_no_signal_flags() noexcept {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

[[nodiscard]] inline ssize_t socket_send(int fd, const void *data, std::size_t size,
                                         int flags) noexcept {
#if defined(__linux__)
    return static_cast<ssize_t>(::syscall(SYS_sendto, fd, data, size, flags, nullptr, 0));
#else
    return ::send(fd, data, size, flags);
#endif
}

[[nodiscard]] inline ssize_t socket_sendmsg(int fd, const msghdr *message, int flags) noexcept {
#if defined(__linux__)
    return static_cast<ssize_t>(::syscall(SYS_sendmsg, fd, message, flags));
#else
    return ::sendmsg(fd, message, flags);
#endif
}

[[nodiscard]] inline int accept_nonblocking(int listener_fd, sockaddr *address,
                                            socklen_t *address_size) noexcept {
#if defined(__linux__)
    return ::accept4(listener_fd, address, address_size, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const int fd = ::accept(listener_fd, address, address_size);
    if (fd >= 0 && (!set_nonblocking(fd) || !set_cloexec(fd))) {
        const int error = errno == 0 ? EIO : errno;
        ::close(fd);
        errno = error;
        return -1;
    }
    return fd;
#endif
}

inline void set_no_sigpipe(int fd) noexcept {
#if defined(SO_NOSIGPIPE)
    int one = 1;
    static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)));
#else
    static_cast<void>(fd);
#endif
}

[[nodiscard]] inline bool set_tcp_no_delay(int fd, bool enabled) noexcept {
    int value = enabled ? 1 : 0;
    return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) == 0;
}

[[nodiscard]] inline bool set_socket_keepalive(int fd, bool enabled) noexcept {
    int value = enabled ? 1 : 0;
    return ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)) == 0;
}

inline void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

} // namespace af::net::detail
