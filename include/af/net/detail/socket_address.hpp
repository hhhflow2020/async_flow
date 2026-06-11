#pragma once

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>

#include "af/net/endpoint.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace af::detail {

struct socket_address {
    sockaddr_storage storage{};
    socklen_t size{0};
    int family{AF_UNSPEC};
};

[[nodiscard]] inline bool socket_address_from_endpoint(const af::net::tcp_endpoint &endpoint,
                                                       socket_address &address,
                                                       int &error) noexcept {
    address = socket_address{};
    error = 0;

    const bool prefer_ipv6 = endpoint.family == af::net::address_family::ipv6 ||
                             (endpoint.family == af::net::address_family::unspecified &&
                              endpoint.address.find(':') != std::string::npos);
    if (endpoint.family == af::net::address_family::unix_domain) {
        if (endpoint.address.empty()) {
            error = EINVAL;
            return false;
        }
        static_assert(sizeof(sockaddr_un) <= sizeof(sockaddr_storage));
        auto *unix_address = reinterpret_cast<sockaddr_un *>(&address.storage);
        unix_address->sun_family = AF_UNIX;
        if (endpoint.address.size() >= sizeof(unix_address->sun_path)) {
            error = ENAMETOOLONG;
            return false;
        }
        std::memcpy(unix_address->sun_path, endpoint.address.data(), endpoint.address.size());
        unix_address->sun_path[endpoint.address.size()] = '\0';
        address.size =
            static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + endpoint.address.size() + 1U);
        address.family = AF_UNIX;
        return true;
    }

    if (prefer_ipv6) {
        auto *ipv6 = reinterpret_cast<sockaddr_in6 *>(&address.storage);
        ipv6->sin6_family = AF_INET6;
        ipv6->sin6_port = htons(endpoint.port);
        if (::inet_pton(AF_INET6, endpoint.address.c_str(), &ipv6->sin6_addr) != 1) {
            error = EINVAL;
            return false;
        }
        address.size = sizeof(sockaddr_in6);
        address.family = AF_INET6;
        return true;
    }

    auto *ipv4 = reinterpret_cast<sockaddr_in *>(&address.storage);
    ipv4->sin_family = AF_INET;
    ipv4->sin_port = htons(endpoint.port);
    if (::inet_pton(AF_INET, endpoint.address.c_str(), &ipv4->sin_addr) != 1) {
        error = EINVAL;
        return false;
    }
    address.size = sizeof(sockaddr_in);
    address.family = AF_INET;
    return true;
}

[[nodiscard]] inline af::net::tcp_endpoint endpoint_from_socket_address(const sockaddr *address,
                                                                        socklen_t size) {
    if (address == nullptr || size == 0) {
        return {};
    }
    char text[INET6_ADDRSTRLEN]{};
    if (address->sa_family == AF_INET && size >= static_cast<socklen_t>(sizeof(sockaddr_in))) {
        const auto *ipv4 = reinterpret_cast<const sockaddr_in *>(address);
        if (::inet_ntop(AF_INET, &ipv4->sin_addr, text, sizeof(text)) == nullptr) {
            return {};
        }
        return af::net::tcp_endpoint::host(text, ntohs(ipv4->sin_port),
                                           af::net::address_family::ipv4);
    }
    if (address->sa_family == AF_INET6 && size >= static_cast<socklen_t>(sizeof(sockaddr_in6))) {
        const auto *ipv6 = reinterpret_cast<const sockaddr_in6 *>(address);
        if (::inet_ntop(AF_INET6, &ipv6->sin6_addr, text, sizeof(text)) == nullptr) {
            return {};
        }
        return af::net::tcp_endpoint::host(text, ntohs(ipv6->sin6_port),
                                           af::net::address_family::ipv6);
    }
    if (address->sa_family == AF_UNIX && size > offsetof(sockaddr_un, sun_path)) {
        const auto *unix_address = reinterpret_cast<const sockaddr_un *>(address);
        const std::size_t max_size =
            static_cast<std::size_t>(size - offsetof(sockaddr_un, sun_path));
        std::size_t path_size = 0;
        while (path_size < max_size && unix_address->sun_path[path_size] != '\0') {
            ++path_size;
        }
        return af::net::tcp_endpoint::unix_path(std::string(unix_address->sun_path, path_size));
    }
    return {};
}

} // namespace af::detail
