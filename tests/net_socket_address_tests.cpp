#include <cstdint>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/detail/net/socket_address.hpp"
#include "af/net.hpp"
#include "af/net/tcp_endpoint.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/un.h>

namespace {

struct NetAliasRuntime;

} // namespace

TEST(NetSocketAddressTests, LowerCaseNetAliasesMatchPublicTypes) {
    static_assert(std::is_same_v<af::net::address_family, af::net::AddressFamily>);
    static_assert(std::is_same_v<af::net::ip_endpoint, af::net::IpEndpoint>);
    static_assert(std::is_same_v<af::net::tcp_endpoint, af::net::TcpEndpoint>);
    static_assert(std::is_same_v<af::net::udp_endpoint, af::net::UdpEndpoint>);
    static_assert(std::is_same_v<af::net::unix_endpoint, af::net::UnixEndpoint>);

    static_assert(std::is_same_v<af::net::send_result, af::net::SendResult>);
    static_assert(std::is_same_v<af::net::close_reason, af::net::CloseReason>);
    static_assert(std::is_same_v<af::net::accept_strategy, af::net::AcceptStrategy>);
    static_assert(std::is_same_v<af::net::listener_state, af::net::ListenerState>);
    static_assert(std::is_same_v<af::net::remove_listener_policy, af::net::RemoveListenerPolicy>);
    static_assert(std::is_same_v<af::net::tcp_listener_options, af::net::TcpListenerOptions>);
    static_assert(std::is_same_v<af::net::tcp_server_config, af::net::TcpServerConfig>);
    static_assert(std::is_same_v<af::net::listener_id, af::net::ListenerId>);
    static_assert(std::is_same_v<af::net::tcp_listener_handle, af::net::TcpListenerHandle>);
    static_assert(std::is_same_v<af::net::listener_result, af::net::ListenerResult>);
    static_assert(std::is_same_v<af::net::tcp_client_options, af::net::TcpClientOptions>);
    static_assert(
        std::is_same_v<af::net::tcp_client_runtime_config, af::net::TcpClientRuntimeConfig>);
    static_assert(std::is_same_v<af::net::udp_send_result, af::net::UdpSendResult>);
    static_assert(std::is_same_v<af::net::udp_socket_options, af::net::UdpSocketOptions>);
    static_assert(
        std::is_same_v<af::net::udp_socket_runtime_config, af::net::UdpSocketRuntimeConfig>);
    static_assert(std::is_same_v<af::net::udp_peer, af::net::UdpPeer>);

    static_assert(
        std::is_same_v<af::net::tcp_server<NetAliasRuntime>, af::net::TcpServer<NetAliasRuntime>>);
    static_assert(
        std::is_same_v<af::net::tcp_client<NetAliasRuntime>, af::net::TcpClient<NetAliasRuntime>>);
    static_assert(
        std::is_same_v<af::net::udp_socket<NetAliasRuntime>, af::net::UdpSocket<NetAliasRuntime>>);
    static_assert(std::is_same_v<af::net::unix_stream_server<NetAliasRuntime>,
                                 af::net::UnixStreamServer<NetAliasRuntime>>);
    static_assert(std::is_same_v<af::net::unix_stream_client<NetAliasRuntime>,
                                 af::net::UnixStreamClient<NetAliasRuntime>>);
    static_assert(std::is_same_v<af::net::unix_datagram_socket<NetAliasRuntime>,
                                 af::net::UnixDatagramSocket<NetAliasRuntime>>);

    static_assert(af::net::address_family::unspecified == af::net::AddressFamily::Unspecified);
    static_assert(af::net::address_family::ipv4 == af::net::AddressFamily::IPv4);
    static_assert(af::net::address_family::ipv6 == af::net::AddressFamily::IPv6);
    static_assert(af::net::address_family::unix_domain == af::net::AddressFamily::Unix);
    static_assert(af::net::send_result::accepted == af::net::SendResult::Accepted);
    static_assert(af::net::send_result::queued == af::net::SendResult::Queued);
    static_assert(af::net::send_result::backpressure == af::net::SendResult::Backpressure);
    static_assert(af::net::send_result::closed == af::net::SendResult::Closed);
    static_assert(af::net::send_result::unsupported == af::net::SendResult::Unsupported);
    static_assert(af::net::close_reason::local == af::net::CloseReason::Local);
    static_assert(af::net::close_reason::peer == af::net::CloseReason::Peer);
    static_assert(af::net::close_reason::error == af::net::CloseReason::Error);
    static_assert(af::net::accept_strategy::auto_select == af::net::AcceptStrategy::Auto);
    static_assert(af::net::accept_strategy::reuse_port_per_io_thread ==
                  af::net::AcceptStrategy::ReusePortPerIoThread);
    static_assert(af::net::accept_strategy::single_acceptor ==
                  af::net::AcceptStrategy::SingleAcceptor);
    static_assert(af::net::listener_state::configured == af::net::ListenerState::Configured);
    static_assert(af::net::listener_state::starting == af::net::ListenerState::Starting);
    static_assert(af::net::listener_state::active == af::net::ListenerState::Active);
    static_assert(af::net::listener_state::failed == af::net::ListenerState::Failed);
    static_assert(af::net::listener_state::removed == af::net::ListenerState::Removed);
    static_assert(af::net::remove_listener_policy::stop_accept_only ==
                  af::net::RemoveListenerPolicy::StopAcceptOnly);
    static_assert(af::net::remove_listener_policy::close_existing_connections ==
                  af::net::RemoveListenerPolicy::CloseExistingConnections);
    static_assert(af::net::udp_send_result::accepted == af::net::UdpSendResult::Accepted);
    static_assert(af::net::udp_send_result::queued == af::net::UdpSendResult::Queued);
    static_assert(af::net::udp_send_result::backpressure == af::net::UdpSendResult::Backpressure);
    static_assert(af::net::udp_send_result::closed == af::net::UdpSendResult::Closed);
    static_assert(af::net::udp_send_result::unsupported == af::net::UdpSendResult::Unsupported);
}

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

TEST(NetSocketAddressTests, ConvertsUnixEndpointToSocketAddress) {
    const af::net::UnixEndpoint endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-test.sock");
    af::detail::SocketAddress address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(endpoint, address, error));
    EXPECT_EQ(error, 0);
    EXPECT_EQ(address.family, AF_UNIX);

    const auto *unix_address = reinterpret_cast<const sockaddr_un *>(&address.storage);
    EXPECT_EQ(unix_address->sun_family, AF_UNIX);
    EXPECT_STREQ(unix_address->sun_path, "/tmp/af-test.sock");

    const af::net::UnixEndpoint roundtrip = af::detail::endpoint_from_socket_address(
        reinterpret_cast<const sockaddr *>(unix_address), address.size);
    EXPECT_EQ(roundtrip.address, "/tmp/af-test.sock");
    EXPECT_EQ(roundtrip.family, af::net::AddressFamily::Unix);
}
