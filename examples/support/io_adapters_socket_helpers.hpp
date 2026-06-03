#pragma once

#include "io_adapters_runtime.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace io_adapters_example {

struct UdpLoopbackEndpoint {
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

inline bool apply_adapter_socket_flags(int fd) noexcept {
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

[[nodiscard]] inline int adapter_socket_type(int base) noexcept {
#if defined(SOCK_NONBLOCK)
    base |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    base |= SOCK_CLOEXEC;
#endif
    return base;
}

struct StreamSocketPair {
    af::UniqueFd server{};
    af::UniqueFd client{};

    bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, adapter_socket_type(SOCK_STREAM), 0, fds) != 0) {
            return false;
        }
        server.reset(fds[0]);
        client.reset(fds[1]);
        if (!apply_adapter_socket_flags(server.get()) ||
            !apply_adapter_socket_flags(client.get())) {
            return false;
        }
        return true;
    }
};

struct UdpLoopbackSockets {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() noexcept {
        receiver.reset(::socket(AF_INET, adapter_socket_type(SOCK_DGRAM), 0));
        sender.reset(::socket(AF_INET, adapter_socket_type(SOCK_DGRAM), 0));
        if (!receiver || !sender || !apply_adapter_socket_flags(receiver.get()) ||
            !apply_adapter_socket_flags(sender.get())) {
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

    [[nodiscard]] const UdpLoopbackEndpoint &endpoint() const noexcept {
        return endpoint_;
    }

private:
    UdpLoopbackEndpoint endpoint_{};
};

} // namespace io_adapters_example
