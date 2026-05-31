#pragma once

#include "io_tcp_echo_runtime.hpp"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
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
    return ::setsockopt(
        fd,
        SOL_SOCKET,
        SO_REUSEADDR,
        &enabled,
        static_cast<socklen_t>(sizeof(enabled))) == 0;
}

struct EchoLoopbackListener {
    af::UniqueFd fd{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};

    bool create() noexcept {
        fd = echo_make_tcp_socket();
        if (!fd || !echo_set_reuse_addr(fd.get())) {
            return false;
        }

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(fd.get(), 64) != 0) {
            return false;
        }

        address_size = sizeof(address);
        return ::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
    }
};

} // namespace io_tcp_echo_example

#endif
