#pragma once

#include "io_uring_datagram_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_datagram_example {

inline af::UniqueFd make_udp_socket() noexcept {
    af::UniqueFd fd(::socket(AF_INET, asyncflow_examples::socket_type_with_flags(SOCK_DGRAM), 0));
    if (!fd || !asyncflow_examples::apply_socket_flags(fd.get())) {
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
