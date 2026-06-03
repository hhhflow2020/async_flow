#pragma once

#include "io_tcp_connect_accept_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_connect_accept_example {

inline af::UniqueFd make_tcp_socket() noexcept {
    af::UniqueFd fd(::socket(AF_INET, asyncflow_examples::socket_type_with_flags(SOCK_STREAM), 0));
    if (!fd || !asyncflow_examples::apply_socket_flags(fd.get())) {
        return {};
    }
    return fd;
}

inline bool set_reuse_addr(int fd) noexcept {
    const int enabled = 1;
    return ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled,
                        static_cast<socklen_t>(sizeof(enabled))) == 0;
}

struct TcpLoopbackSockets {
    af::UniqueFd listener{};
    af::UniqueFd client{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};

    bool create() noexcept {
        listener = make_tcp_socket();
        client = make_tcp_socket();
        if (!listener || !client || !set_reuse_addr(listener.get())) {
            return false;
        }

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(listener.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
            ::listen(listener.get(), 16) != 0) {
            return false;
        }

        address_size = sizeof(address);
        return ::getsockname(listener.get(), reinterpret_cast<sockaddr *>(&address),
                             &address_size) == 0;
    }
};

} // namespace io_tcp_connect_accept_example
