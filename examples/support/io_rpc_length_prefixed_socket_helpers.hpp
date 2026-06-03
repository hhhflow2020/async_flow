#pragma once

#include "io_rpc_length_prefixed_runtime.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace io_rpc_length_prefixed_example {

struct RpcLoopbackEndpoint {
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

inline bool apply_rpc_socket_flags(int fd) noexcept {
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

[[nodiscard]] inline int rpc_socket_type() noexcept {
    int type = SOCK_STREAM;
#if defined(SOCK_NONBLOCK)
    type |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    type |= SOCK_CLOEXEC;
#endif
    return type;
}

[[nodiscard]] inline af::UniqueFd make_rpc_socket() noexcept {
    af::UniqueFd fd(::socket(AF_INET, rpc_socket_type(), 0));
    if (!fd || !apply_rpc_socket_flags(fd.get())) {
        return {};
    }
    return fd;
}

struct RpcLoopbackSockets {
    [[nodiscard]] bool create() noexcept {
        listener = make_rpc_socket();
        client = make_rpc_socket();
        if (!listener || !client) {
            return false;
        }

        endpoint_.address = sockaddr_in{};
        endpoint_.address.sin_family = AF_INET;
        endpoint_.address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        endpoint_.address.sin_port = 0;
        if (::bind(listener.get(), reinterpret_cast<sockaddr *>(&endpoint_.address),
                   sizeof(endpoint_.address)) != 0 ||
            ::listen(listener.get(), 16) != 0) {
            return false;
        }

        endpoint_.address_size = sizeof(endpoint_.address);
        return ::getsockname(listener.get(), reinterpret_cast<sockaddr *>(&endpoint_.address),
                             &endpoint_.address_size) == 0;
    }

    [[nodiscard]] const RpcLoopbackEndpoint &endpoint() const noexcept {
        return endpoint_;
    }

    af::UniqueFd listener{};
    af::UniqueFd client{};

private:
    RpcLoopbackEndpoint endpoint_{};
};

} // namespace io_rpc_length_prefixed_example
