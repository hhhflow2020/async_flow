#pragma once

#include "io_tcp_echo_runtime.hpp"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace io_tcp_echo_example {

inline bool echo_apply_socket_flags(int fd) noexcept {
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

inline af::UniqueFd echo_make_tcp_socket() noexcept {
    int type = SOCK_STREAM;
#if defined(SOCK_NONBLOCK)
    type |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    type |= SOCK_CLOEXEC;
#endif

    af::UniqueFd fd(::socket(AF_INET, type, 0));
    if (!fd || !echo_apply_socket_flags(fd.get())) {
        return {};
    }
    return fd;
}

inline bool echo_set_reuse_addr(int fd) noexcept {
    const int enabled = 1;
    return ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled,
                        static_cast<socklen_t>(sizeof(enabled))) == 0;
}

inline bool echo_set_tcp_nodelay(int fd) noexcept {
    const int enabled = 1;
    return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                        static_cast<socklen_t>(sizeof(enabled))) == 0;
}

inline bool echo_set_keepalive(int fd) noexcept {
    const int enabled = 1;
    return ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled,
                        static_cast<socklen_t>(sizeof(enabled))) == 0;
}

struct EchoTcpListener {
    af::UniqueFd fd{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
    int error{0};

    bool create(const char *bind_address, std::uint16_t port, int backlog) noexcept {
        error = 0;
        fd = echo_make_tcp_socket();
        if (!fd || !echo_set_reuse_addr(fd.get())) {
            error = errno == 0 ? EIO : errno;
            return false;
        }

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        if (::inet_pton(AF_INET, bind_address, &address.sin_addr) != 1) {
            error = EINVAL;
            return false;
        }

        if (::bind(fd.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
            ::listen(fd.get(), backlog) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }

        address_size = sizeof(address);
        if (::getsockname(fd.get(), reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
        return true;
    }
};

[[nodiscard]] inline sockaddr_in echo_loopback_target(sockaddr_in address) noexcept {
    if (address.sin_addr.s_addr == htonl(INADDR_ANY)) {
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    return address;
}

[[nodiscard]] inline bool echo_wait_connected(int fd) noexcept {
    pollfd descriptor{fd, POLLOUT, 0};
    for (;;) {
        const int ready = ::poll(&descriptor, 1, 100);
        if (ready > 0) {
            break;
        }
        if (ready == 0) {
            return false;
        }
        if (errno != EINTR) {
            return false;
        }
    }

    int error = 0;
    socklen_t error_size = sizeof(error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
        return false;
    }
    return error == 0 || error == EISCONN;
}

inline bool echo_wake_listener(sockaddr_in address) noexcept {
    af::UniqueFd fd = echo_make_tcp_socket();
    if (!fd) {
        return false;
    }

    const sockaddr_in target = echo_loopback_target(address);
    for (;;) {
        if (::connect(fd.get(), reinterpret_cast<const sockaddr *>(&target), sizeof(target)) == 0) {
            return true;
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (error == EINPROGRESS || error == EALREADY || error == EWOULDBLOCK) {
            return echo_wait_connected(fd.get());
        }
        return false;
    }
}

} // namespace io_tcp_echo_example

#endif
