#pragma once

#include "../app_runtime.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace io_adapters_example {

struct UdpLoopbackEndpoint {
#if defined(__linux__)
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
#endif
};

#if defined(__linux__)

struct StreamSocketPair {
    af::UniqueFd server{};
    af::UniqueFd client{};

    bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
            return false;
        }
        server.reset(fds[0]);
        client.reset(fds[1]);
        return true;
    }
};

struct UdpLoopbackSockets {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() noexcept {
        receiver.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        sender.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!receiver || !sender) {
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

#else

struct StreamSocketPair {
    af::UniqueFd server{};
    af::UniqueFd client{};

    bool create() noexcept {
        return false;
    }
};

struct UdpLoopbackSockets {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() noexcept {
        return false;
    }

    [[nodiscard]] const UdpLoopbackEndpoint &endpoint() const noexcept {
        return endpoint_;
    }

private:
    UdpLoopbackEndpoint endpoint_{};
};

#endif

} // namespace io_adapters_example
