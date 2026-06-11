#pragma once

#include <cstdint>
#include <string>
#include <utility>

namespace af::net {

enum class address_family : std::uint8_t {
    unspecified,
    ipv4,
    ipv6,
    unix_domain,
    Unspecified = unspecified,
    IPv4 = ipv4,
    IPv6 = ipv6,
    Unix = unix_domain,
};

struct ip_endpoint {
    std::string address{"0.0.0.0"};
    std::uint16_t port{0};
    address_family family{address_family::ipv4};

    [[nodiscard]] static ip_endpoint any(std::uint16_t port) {
        return any_v4(port);
    }

    [[nodiscard]] static ip_endpoint loopback(std::uint16_t port) {
        return loopback_v4(port);
    }

    [[nodiscard]] static ip_endpoint any_v4(std::uint16_t port) {
        return {"0.0.0.0", port, address_family::ipv4};
    }

    [[nodiscard]] static ip_endpoint any_v6(std::uint16_t port) {
        return {"::", port, address_family::ipv6};
    }

    [[nodiscard]] static ip_endpoint loopback_v4(std::uint16_t port) {
        return {"127.0.0.1", port, address_family::ipv4};
    }

    [[nodiscard]] static ip_endpoint loopback_v6(std::uint16_t port) {
        return {"::1", port, address_family::ipv6};
    }

    [[nodiscard]] static ip_endpoint host(std::string address, std::uint16_t port,
                                          address_family family = address_family::unspecified) {
        return {std::move(address), port, family};
    }

    [[nodiscard]] static ip_endpoint unix_path(std::string path) {
        return {std::move(path), 0, address_family::unix_domain};
    }
};

using tcp_endpoint = ip_endpoint;
using udp_endpoint = ip_endpoint;
using unix_endpoint = ip_endpoint;

using AddressFamily = address_family;
using IpEndpoint = ip_endpoint;
using TcpEndpoint = tcp_endpoint;
using UdpEndpoint = udp_endpoint;
using UnixEndpoint = unix_endpoint;

} // namespace af::net
