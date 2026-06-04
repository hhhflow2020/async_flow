#pragma once

#include <cstdint>
#include <string>
#include <utility>

namespace af::net {

enum class AddressFamily : std::uint8_t {
    Unspecified,
    IPv4,
    IPv6,
    Unix,
};

struct IpEndpoint {
    std::string address{"0.0.0.0"};
    std::uint16_t port{0};
    AddressFamily family{AddressFamily::IPv4};

    [[nodiscard]] static IpEndpoint any(std::uint16_t port) {
        return any_v4(port);
    }

    [[nodiscard]] static IpEndpoint loopback(std::uint16_t port) {
        return loopback_v4(port);
    }

    [[nodiscard]] static IpEndpoint any_v4(std::uint16_t port) {
        return {"0.0.0.0", port, AddressFamily::IPv4};
    }

    [[nodiscard]] static IpEndpoint any_v6(std::uint16_t port) {
        return {"::", port, AddressFamily::IPv6};
    }

    [[nodiscard]] static IpEndpoint loopback_v4(std::uint16_t port) {
        return {"127.0.0.1", port, AddressFamily::IPv4};
    }

    [[nodiscard]] static IpEndpoint loopback_v6(std::uint16_t port) {
        return {"::1", port, AddressFamily::IPv6};
    }

    [[nodiscard]] static IpEndpoint host(std::string address, std::uint16_t port,
                                         AddressFamily family = AddressFamily::Unspecified) {
        return {std::move(address), port, family};
    }

    [[nodiscard]] static IpEndpoint unix_path(std::string path) {
        return {std::move(path), 0, AddressFamily::Unix};
    }
};

using TcpEndpoint = IpEndpoint;
using UdpEndpoint = IpEndpoint;
using UnixEndpoint = IpEndpoint;

} // namespace af::net
