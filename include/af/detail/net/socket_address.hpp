#pragma once

#include <cerrno>
#include <cstring>

#include "af/net/tcp_endpoint.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace af::detail {

struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t size{0};
    int family{AF_UNSPEC};
};

[[nodiscard]] inline bool socket_address_from_endpoint(const af::net::TcpEndpoint &endpoint,
                                                       SocketAddress &address,
                                                       int &error) noexcept {
    address = SocketAddress{};
    error = 0;

    const bool prefer_ipv6 = endpoint.family == af::net::AddressFamily::IPv6 ||
                             (endpoint.family == af::net::AddressFamily::Unspecified &&
                              endpoint.address.find(':') != std::string::npos);
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

[[nodiscard]] inline af::net::TcpEndpoint endpoint_from_socket_address(const sockaddr *address,
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
        return af::net::TcpEndpoint::host(text, ntohs(ipv4->sin_port),
                                          af::net::AddressFamily::IPv4);
    }
    if (address->sa_family == AF_INET6 && size >= static_cast<socklen_t>(sizeof(sockaddr_in6))) {
        const auto *ipv6 = reinterpret_cast<const sockaddr_in6 *>(address);
        if (::inet_ntop(AF_INET6, &ipv6->sin6_addr, text, sizeof(text)) == nullptr) {
            return {};
        }
        return af::net::TcpEndpoint::host(text, ntohs(ipv6->sin6_port),
                                          af::net::AddressFamily::IPv6);
    }
    return {};
}

} // namespace af::detail
