#pragma once

#include "io_vectored_runtime.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace io_vectored_example {

struct VectoredUdpEndpoint {
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

inline bool apply_vectored_socket_flags(int fd) noexcept {
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

[[nodiscard]] inline int vectored_socket_type(int base) noexcept {
#if defined(SOCK_NONBLOCK)
    base |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    base |= SOCK_CLOEXEC;
#endif
    return base;
}

struct VectoredStreamSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, vectored_socket_type(SOCK_STREAM), 0, fds) != 0) {
            return false;
        }
        server.reset(fds[0]);
        client.reset(fds[1]);
        if (!apply_vectored_socket_flags(server.get()) ||
            !apply_vectored_socket_flags(client.get())) {
            return false;
        }
        return true;
    }

    af::UniqueFd server{};
    af::UniqueFd client{};
};

struct VectoredUdpLoopbackSockets {
    [[nodiscard]] bool create() noexcept {
        receiver.reset(::socket(AF_INET, vectored_socket_type(SOCK_DGRAM), 0));
        sender.reset(::socket(AF_INET, vectored_socket_type(SOCK_DGRAM), 0));
        if (!receiver || !sender || !apply_vectored_socket_flags(receiver.get()) ||
            !apply_vectored_socket_flags(sender.get())) {
            return false;
        }

        endpoint_.address = sockaddr_in{};
        endpoint_.address.sin_family = AF_INET;
        endpoint_.address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        endpoint_.address.sin_port = 0;
        if (::bind(receiver.get(), reinterpret_cast<sockaddr *>(&endpoint_.address),
                   sizeof(endpoint_.address)) != 0) {
            return false;
        }
        endpoint_.address_size = sizeof(endpoint_.address);
        return ::getsockname(receiver.get(), reinterpret_cast<sockaddr *>(&endpoint_.address),
                             &endpoint_.address_size) == 0;
    }

    [[nodiscard]] const VectoredUdpEndpoint &endpoint() const noexcept {
        return endpoint_;
    }

    af::UniqueFd receiver{};
    af::UniqueFd sender{};

private:
    VectoredUdpEndpoint endpoint_{};
};

} // namespace io_vectored_example
