#pragma once

#include "io_uring_datagram_runtime.hpp"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace io_uring_datagram_example {

inline bool apply_datagram_socket_flags(int fd) noexcept {
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

inline af::UniqueFd make_udp_socket() noexcept {
    int type = SOCK_DGRAM;
#if defined(SOCK_NONBLOCK)
    type |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    type |= SOCK_CLOEXEC;
#endif

    af::UniqueFd fd(::socket(AF_INET, type, 0));
    if (!fd || !apply_datagram_socket_flags(fd.get())) {
        return {};
    }
    return fd;
}

struct DatagramLoopbackSockets {
    af::UniqueFd server{};
    af::UniqueFd client{};
    sockaddr_in server_address{};
    socklen_t server_size{sizeof(server_address)};

    bool create() noexcept {
        server = make_udp_socket();
        client = make_udp_socket();
        if (!server || !client) {
            return false;
        }

        server_address = sockaddr_in{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        server_address.sin_port = 0;
        if (::bind(server.get(), reinterpret_cast<sockaddr *>(&server_address),
                   sizeof(server_address)) != 0) {
            return false;
        }

        server_size = sizeof(server_address);
        return ::getsockname(server.get(), reinterpret_cast<sockaddr *>(&server_address),
                             &server_size) == 0;
    }
};

} // namespace io_uring_datagram_example

#endif
