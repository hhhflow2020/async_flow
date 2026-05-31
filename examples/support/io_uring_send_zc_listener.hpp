#pragma once

#include "io_uring_send_zc_runtime.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_send_zc_example {

struct SendZcLoopbackListener {
    af::UniqueFd fd{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};

    bool create() noexcept {
        fd.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!fd) {
            return false;
        }

        const int one = 1;
        static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        address_size = sizeof(address);
        return ::bind(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0 &&
               ::listen(fd.get(), 8) == 0 &&
               ::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
    }
};

} // namespace io_uring_send_zc_example

#endif
