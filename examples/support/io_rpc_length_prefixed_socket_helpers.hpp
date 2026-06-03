#pragma once

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace io_rpc_length_prefixed_example {

struct RpcLoopbackEndpoint {
#if defined(__linux__)
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
#endif
};

#if defined(__linux__)

struct RpcLoopbackSockets {
    [[nodiscard]] bool create() noexcept {
        listener.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        client.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
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

#else

struct RpcLoopbackSockets {
    [[nodiscard]] bool create() noexcept {
        return false;
    }

    [[nodiscard]] const RpcLoopbackEndpoint &endpoint() const noexcept {
        return endpoint_;
    }

    af::UniqueFd listener{};
    af::UniqueFd client{};

private:
    RpcLoopbackEndpoint endpoint_{};
};

#endif

} // namespace io_rpc_length_prefixed_example
