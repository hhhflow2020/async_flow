#pragma once

#include "io_uring_send_zc_endpoint.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <sys/socket.h>

namespace io_uring_send_zc_example {

struct SendZcLoopbackListener {
    af::UniqueFd fd{};
    SendZcLoopbackEndpoint endpoint{};

    bool create() noexcept {
        fd.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!fd) {
            return false;
        }

        const int one = 1;
        static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));

        endpoint.address = sockaddr_in{};
        endpoint.address.sin_family = AF_INET;
        endpoint.address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        endpoint.address.sin_port = 0;
        endpoint.address_size = sizeof(endpoint.address);
        return ::bind(fd.get(), reinterpret_cast<sockaddr *>(&endpoint.address),
                      sizeof(endpoint.address)) == 0 &&
               ::listen(fd.get(), 8) == 0 &&
               ::getsockname(fd.get(), reinterpret_cast<sockaddr *>(&endpoint.address),
                             &endpoint.address_size) == 0;
    }
};

} // namespace io_uring_send_zc_example

#else

namespace io_uring_send_zc_example {

struct SendZcLoopbackListener {
    af::UniqueFd fd{};
    SendZcLoopbackEndpoint endpoint{};

    bool create() noexcept {
        return false;
    }
};

} // namespace io_uring_send_zc_example

#endif
