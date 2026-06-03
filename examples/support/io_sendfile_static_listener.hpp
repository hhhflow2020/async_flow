#pragma once

#include "io_sendfile_static_runtime.hpp"
#include "posix_socket_flags.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace io_sendfile_static_example {

struct SendfileLoopbackEndpoint {
#if defined(__linux__)
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
#endif
};

#if defined(__linux__)

struct SendfileLoopbackListener {
    af::UniqueFd fd{};
    SendfileLoopbackEndpoint endpoint{};

    bool create() noexcept {
        fd.reset(asyncflow_examples::socket_with_flags(AF_INET, SOCK_STREAM, 0));
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

#else

struct SendfileLoopbackListener {
    af::UniqueFd fd{};
    SendfileLoopbackEndpoint endpoint{};

    bool create() noexcept {
        return false;
    }
};

#endif

} // namespace io_sendfile_static_example
