#include <cstdint>

#include <gtest/gtest.h>

#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_endpoint.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>

TEST(NetSocketAddressTests, ConvertsIpv4EndpointToSocketAddress) {
    const af::net::TcpEndpoint endpoint =
        af::net::TcpEndpoint::host("127.0.0.1", 43210, af::net::AddressFamily::IPv4);
    af::detail::SocketAddress address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(endpoint, address, error));
    EXPECT_EQ(error, 0);
    EXPECT_EQ(address.family, AF_INET);
    ASSERT_EQ(address.size, static_cast<socklen_t>(sizeof(sockaddr_in)));

    const auto *ipv4 = reinterpret_cast<const sockaddr_in *>(&address.storage);
    EXPECT_EQ(ntohs(ipv4->sin_port), 43210U);
    EXPECT_EQ(ipv4->sin_family, AF_INET);
}

TEST(NetSocketAddressTests, ConvertsIpv6EndpointToSocketAddress) {
    const af::net::TcpEndpoint endpoint =
        af::net::TcpEndpoint::host("::1", 44321, af::net::AddressFamily::IPv6);
    af::detail::SocketAddress address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(endpoint, address, error));
    EXPECT_EQ(error, 0);
    EXPECT_EQ(address.family, AF_INET6);
    ASSERT_EQ(address.size, static_cast<socklen_t>(sizeof(sockaddr_in6)));

    const auto *ipv6 = reinterpret_cast<const sockaddr_in6 *>(&address.storage);
    EXPECT_EQ(ntohs(ipv6->sin6_port), 44321U);
    EXPECT_EQ(ipv6->sin6_family, AF_INET6);
}

TEST(NetSocketAddressTests, InfersAddressFamilyForHostFactory) {
    af::detail::SocketAddress address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(af::net::TcpEndpoint::host("::", 1234),
                                                         address, error));
    EXPECT_EQ(address.family, AF_INET6);

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(
        af::net::TcpEndpoint::host("0.0.0.0", 1234), address, error));
    EXPECT_EQ(address.family, AF_INET);
}

TEST(NetSocketAddressTests, ConvertsSocketAddressBackToEndpoint) {
    sockaddr_in6 ipv6{};
    ipv6.sin6_family = AF_INET6;
    ipv6.sin6_port = htons(23456);
    ASSERT_EQ(::inet_pton(AF_INET6, "::1", &ipv6.sin6_addr), 1);

    const af::net::TcpEndpoint endpoint = af::detail::endpoint_from_socket_address(
        reinterpret_cast<const sockaddr *>(&ipv6), sizeof(ipv6));
    EXPECT_EQ(endpoint.address, "::1");
    EXPECT_EQ(endpoint.port, 23456U);
    EXPECT_EQ(endpoint.family, af::net::AddressFamily::IPv6);
}
