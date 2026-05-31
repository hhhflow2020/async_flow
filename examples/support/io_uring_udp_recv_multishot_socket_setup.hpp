#pragma once

#include "io_uring_udp_recv_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_udp_recv_multishot_example {

inline bool bind_loopback(int fd, sockaddr_in& address, socklen_t& address_size) {
    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        return false;
    }
    address_size = sizeof(address);
    return ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
}

} // namespace io_uring_udp_recv_multishot_example

#endif
