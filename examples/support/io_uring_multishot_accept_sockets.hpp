#pragma once

#include <array>
#include <cstddef>

#include "io_uring_multishot_accept_runtime.hpp"
#include "posix_socket_flags.hpp"

#if defined(__linux__)
#include <cerrno>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_multishot_accept_example {

inline af::UniqueFd make_accept_tcp_socket() noexcept {
    return af::UniqueFd(asyncflow_examples::socket_with_flags(AF_INET, SOCK_STREAM, 0));
}

struct MultishotAcceptListener {
    af::UniqueFd fd{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};

    bool listen() noexcept {
        fd = make_accept_tcp_socket();
        if (!fd) {
            return false;
        }

        address = sockaddr_in{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        address_size = sizeof(address);
        return ::bind(fd.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0 &&
               ::listen(fd.get(), 16) == 0 &&
               ::getsockname(fd.get(), reinterpret_cast<sockaddr *>(&address), &address_size) == 0;
    }
};

template <std::size_t Count>
[[nodiscard]] bool connect_accept_clients(const sockaddr_in &address, socklen_t address_size,
                                          std::array<af::UniqueFd, Count> &clients) noexcept {
    for (af::UniqueFd &client : clients) {
        client = make_accept_tcp_socket();
        if (!client) {
            return false;
        }
        const int rc =
            ::connect(client.get(), reinterpret_cast<const sockaddr *>(&address), address_size);
        if (rc != 0 && errno != EINPROGRESS) {
            return false;
        }
    }
    return true;
}

} // namespace io_uring_multishot_accept_example

#else

namespace io_uring_multishot_accept_example {

struct MultishotAcceptListener {
    af::UniqueFd fd{};
    int address{0};
    std::size_t address_size{0};

    bool listen() noexcept {
        return false;
    }
};

template <std::size_t Count>
[[nodiscard]] bool connect_accept_clients(const int &address, std::size_t address_size,
                                          std::array<af::UniqueFd, Count> &clients) noexcept {
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(clients);
    return false;
}

} // namespace io_uring_multishot_accept_example

#endif
