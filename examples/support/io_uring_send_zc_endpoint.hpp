#pragma once

#include "io_uring_send_zc_runtime.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace io_uring_send_zc_example {

struct SendZcLoopbackEndpoint {
#if defined(__linux__)
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
#endif
};

} // namespace io_uring_send_zc_example
