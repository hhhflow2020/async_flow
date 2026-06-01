#pragma once

#include "../app_runtime.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_adapters_example {

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
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};

    bool create() noexcept {
        receiver.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        sender.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!receiver || !sender) {
            return false;
        }

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(receiver.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
            return false;
        }

        address_size = sizeof(address);
        return ::getsockname(receiver.get(), reinterpret_cast<sockaddr *>(&address),
                             &address_size) == 0;
    }
};

} // namespace io_adapters_example

#endif
