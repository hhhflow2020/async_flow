#pragma once

#include "io_rpc_length_prefixed_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_rpc_length_prefixed_example {

struct RpcLoopbackEndpoint {
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

[[nodiscard]] inline af::UniqueFd make_rpc_socket() noexcept {
    af::UniqueFd fd(::socket(AF_INET, asyncflow_examples::socket_type_with_flags(SOCK_STREAM), 0));
    if (!fd || !asyncflow_examples::apply_socket_flags(fd.get())) {
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
