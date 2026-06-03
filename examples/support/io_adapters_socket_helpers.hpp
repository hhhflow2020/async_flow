#pragma once

#include "io_adapters_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_adapters_example {

struct UdpLoopbackEndpoint {
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

struct StreamSocketPair {
    af::UniqueFd server{};
    af::UniqueFd client{};

    bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, asyncflow_examples::socket_type_with_flags(SOCK_STREAM), 0,
                         fds) != 0) {
            return false;
        }
        server.reset(fds[0]);
        client.reset(fds[1]);
        if (!asyncflow_examples::apply_socket_flags(server.get()) ||
            !asyncflow_examples::apply_socket_flags(client.get())) {
            return false;
        }
        return true;
    }
};

struct UdpLoopbackSockets {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() noexcept {
        receiver.reset(
            ::socket(AF_INET, asyncflow_examples::socket_type_with_flags(SOCK_DGRAM), 0));
        sender.reset(::socket(AF_INET, asyncflow_examples::socket_type_with_flags(SOCK_DGRAM), 0));
        if (!receiver || !sender || !asyncflow_examples::apply_socket_flags(receiver.get()) ||
            !asyncflow_examples::apply_socket_flags(sender.get())) {
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
