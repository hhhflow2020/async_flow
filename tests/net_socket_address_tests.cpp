#include <array>
#include <cstdint>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "af/detail/net/socket_address.hpp"
#include "af/net.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/thread_list.hpp"
#include "af/runtime_config.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/un.h>

namespace {

struct NetAliasRuntime;

#define AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(name, value)                                            \
    template <typename EnumT, typename = void> struct name : std::false_type {};                   \
    template <typename EnumT>                                                                      \
    struct name<EnumT, std::void_t<decltype(EnumT::value)>> : std::true_type {}

AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_unspecified_value, Unspecified);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_ipv4_value, IPv4);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_ipv6_value, IPv6);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_unix_value, Unix);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_accepted_value, Accepted);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_queued_value, Queued);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_backpressure_value, Backpressure);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_closed_value, Closed);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_unsupported_value, Unsupported);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_local_value, Local);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_peer_value, Peer);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_error_value, Error);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_auto_value, Auto);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_reuse_port_per_io_thread_value, ReusePortPerIoThread);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_single_acceptor_value, SingleAcceptor);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_configured_value, Configured);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_starting_value, Starting);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_active_value, Active);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_failed_value, Failed);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_removed_value, Removed);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_stop_accept_only_value, StopAcceptOnly);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_close_existing_connections_value, CloseExistingConnections);

#undef AF_TEST_DEFINE_ENUM_VALUE_DETECTOR

} // namespace

TEST(NetSocketAddressTests, LowerCaseNetAliasesMatchPublicTypes) {
    static_assert(std::is_class_v<af::detail::socket_address>);

    static_assert(std::is_enum_v<af::net::address_family>);
    static_assert(std::is_same_v<af::net::ip_endpoint, af::net::tcp_endpoint>);
    static_assert(std::is_same_v<af::net::ip_endpoint, af::net::udp_endpoint>);
    static_assert(std::is_same_v<af::net::ip_endpoint, af::net::unix_endpoint>);

    static_assert(std::is_enum_v<af::net::send_result>);
    static_assert(std::is_enum_v<af::net::close_reason>);
    static_assert(std::is_same_v<af::net::tcp_accept_strategy, af::net::accept_strategy>);
    static_assert(std::is_enum_v<af::net::listener_state>);
    static_assert(std::is_enum_v<af::net::remove_listener_policy>);
    static_assert(std::is_class_v<af::net::tcp_listener_options>);
    static_assert(std::is_class_v<af::net::tcp_connection_config>);
    static_assert(std::is_class_v<af::net::tcp_listener_config>);
    static_assert(std::is_class_v<af::net::tcp_server_config>);
    static_assert(std::is_class_v<af::net::listener_id>);
    static_assert(std::is_class_v<af::net::tcp_listener_handle>);
    static_assert(std::is_class_v<af::net::listener_result>);
    static_assert(std::is_class_v<af::net::tcp_connection_ref>);
    static_assert(std::is_class_v<af::net::tcp_connection_handle>);
    static_assert(std::is_class_v<af::net::tcp_client>);
    static_assert(std::is_class_v<af::net::tcp_client_options>);
    static_assert(std::is_class_v<af::net::tcp_client_runtime_config>);
    static_assert(std::is_enum_v<af::net::udp_send_result>);
    static_assert(std::is_class_v<af::net::udp_socket_options>);
    static_assert(std::is_class_v<af::net::udp_socket_runtime_config>);
    static_assert(std::is_class_v<af::net::udp_peer>);

    static_assert(std::is_class_v<af::net::tcp_server>);
    static_assert(std::is_class_v<af::net::udp_socket>);
    static_assert(std::is_class_v<af::net::udp_socket_handle>);
    static_assert(std::is_class_v<af::net::udp_socket_ref>);
    static_assert(std::is_same_v<af::net::unix_connection_ref, af::net::tcp_connection_ref>);
    static_assert(std::is_same_v<af::net::unix_connection_handle, af::net::tcp_connection_handle>);
    static_assert(std::is_same_v<af::net::unix_datagram_socket_ref, af::net::udp_socket_ref>);
    static_assert(std::is_same_v<af::net::unix_datagram_socket_handle, af::net::udp_socket_handle>);
    static_assert(std::is_same_v<af::net::unix_datagram_peer, af::net::udp_peer>);
    static_assert(std::is_class_v<af::net::unix_stream_server>);
    static_assert(std::is_class_v<af::net::unix_stream_client>);
    static_assert(std::is_class_v<af::net::unix_datagram_socket>);

    static_assert(af::net::address_family::unspecified == af::net::address_family::unspecified);
    static_assert(af::net::address_family::ipv4 == af::net::address_family::ipv4);
    static_assert(af::net::address_family::ipv6 == af::net::address_family::ipv6);
    static_assert(af::net::address_family::unix_domain == af::net::address_family::unix_domain);
    static_assert(!has_unspecified_value<af::net::address_family>::value);
    static_assert(!has_ipv4_value<af::net::address_family>::value);
    static_assert(!has_ipv6_value<af::net::address_family>::value);
    static_assert(!has_unix_value<af::net::address_family>::value);
    static_assert(af::net::send_result::accepted == af::net::send_result::accepted);
    static_assert(af::net::send_result::queued == af::net::send_result::queued);
    static_assert(af::net::send_result::backpressure == af::net::send_result::backpressure);
    static_assert(af::net::send_result::closed == af::net::send_result::closed);
    static_assert(af::net::send_result::unsupported == af::net::send_result::unsupported);
    static_assert(!has_accepted_value<af::net::send_result>::value);
    static_assert(!has_queued_value<af::net::send_result>::value);
    static_assert(!has_backpressure_value<af::net::send_result>::value);
    static_assert(!has_closed_value<af::net::send_result>::value);
    static_assert(!has_unsupported_value<af::net::send_result>::value);
    static_assert(af::net::close_reason::local == af::net::close_reason::local);
    static_assert(af::net::close_reason::peer == af::net::close_reason::peer);
    static_assert(af::net::close_reason::error == af::net::close_reason::error);
    static_assert(!has_local_value<af::net::close_reason>::value);
    static_assert(!has_peer_value<af::net::close_reason>::value);
    static_assert(!has_error_value<af::net::close_reason>::value);
    static_assert(af::net::tcp_accept_strategy::auto_select ==
                  af::net::tcp_accept_strategy::auto_select);
    static_assert(af::net::tcp_accept_strategy::reuse_port_per_io_thread ==
                  af::net::tcp_accept_strategy::reuse_port_per_io_thread);
    static_assert(af::net::tcp_accept_strategy::single_acceptor ==
                  af::net::tcp_accept_strategy::single_acceptor);
    static_assert(!has_auto_value<af::net::tcp_accept_strategy>::value);
    static_assert(!has_reuse_port_per_io_thread_value<af::net::tcp_accept_strategy>::value);
    static_assert(!has_single_acceptor_value<af::net::tcp_accept_strategy>::value);
    static_assert(af::net::listener_state::configured == af::net::listener_state::configured);
    static_assert(af::net::listener_state::starting == af::net::listener_state::starting);
    static_assert(af::net::listener_state::active == af::net::listener_state::active);
    static_assert(af::net::listener_state::failed == af::net::listener_state::failed);
    static_assert(af::net::listener_state::removed == af::net::listener_state::removed);
    static_assert(!has_configured_value<af::net::listener_state>::value);
    static_assert(!has_starting_value<af::net::listener_state>::value);
    static_assert(!has_active_value<af::net::listener_state>::value);
    static_assert(!has_failed_value<af::net::listener_state>::value);
    static_assert(!has_removed_value<af::net::listener_state>::value);
    static_assert(af::net::remove_listener_policy::stop_accept_only ==
                  af::net::remove_listener_policy::stop_accept_only);
    static_assert(af::net::remove_listener_policy::close_existing_connections ==
                  af::net::remove_listener_policy::close_existing_connections);
    static_assert(!has_stop_accept_only_value<af::net::remove_listener_policy>::value);
    static_assert(!has_close_existing_connections_value<af::net::remove_listener_policy>::value);
    static_assert(af::net::udp_send_result::accepted == af::net::udp_send_result::accepted);
    static_assert(af::net::udp_send_result::queued == af::net::udp_send_result::queued);
    static_assert(af::net::udp_send_result::backpressure == af::net::udp_send_result::backpressure);
    static_assert(af::net::udp_send_result::closed == af::net::udp_send_result::closed);
    static_assert(af::net::udp_send_result::unsupported == af::net::udp_send_result::unsupported);
    static_assert(!has_accepted_value<af::net::udp_send_result>::value);
    static_assert(!has_queued_value<af::net::udp_send_result>::value);
    static_assert(!has_backpressure_value<af::net::udp_send_result>::value);
    static_assert(!has_closed_value<af::net::udp_send_result>::value);
    static_assert(!has_unsupported_value<af::net::udp_send_result>::value);
}

TEST(NetSocketAddressTests, ThreadListCopiesRuntimeThreadGroupRef) {
    const std::array<std::uint16_t, 3> indexes{2, 4, 9};
    const af::thread_group_ref group(indexes.data(), indexes.size());

    const std::vector<af::thread_ref> threads = af::net::thread_list(group);

    ASSERT_EQ(threads.size(), indexes.size());
    EXPECT_EQ(threads[0], af::thread_ref(2));
    EXPECT_EQ(threads[1], af::thread_ref(4));
    EXPECT_EQ(threads[2], af::thread_ref(9));
}

TEST(NetSocketAddressTests, TcpListenerConfigAcceptsRuntimeThreadRefs) {
    const std::array<std::uint16_t, 2> indexes{1, 3};
    af::net::tcp_listener_config config;

    config.name = "public";
    config.endpoint = af::net::tcp_endpoint::any(8080);
    config.threads = af::net::thread_list(af::thread_group_ref(indexes.data(), indexes.size()));
    config.options.reuse_port = true;
    config.accept_strategy = af::net::tcp_accept_strategy::reuse_port_per_io_thread;

    EXPECT_EQ(config.name, "public");
    EXPECT_EQ(config.endpoint.port, 8080U);
    ASSERT_EQ(config.threads.size(), indexes.size());
    EXPECT_EQ(config.threads[0], af::thread_ref(1));
    EXPECT_EQ(config.threads[1], af::thread_ref(3));
    EXPECT_TRUE(config.options.reuse_port);
    EXPECT_EQ(config.accept_strategy, af::net::tcp_accept_strategy::reuse_port_per_io_thread);
}

TEST(NetSocketAddressTests, ConvertsIpv4EndpointToSocketAddress) {
    const af::net::tcp_endpoint endpoint =
        af::net::tcp_endpoint::host("127.0.0.1", 43210, af::net::address_family::ipv4);
    af::detail::socket_address address{};
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
    const af::net::tcp_endpoint endpoint =
        af::net::tcp_endpoint::host("::1", 44321, af::net::address_family::ipv6);
    af::detail::socket_address address{};
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
    af::detail::socket_address address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(af::net::tcp_endpoint::host("::", 1234),
                                                         address, error));
    EXPECT_EQ(address.family, AF_INET6);

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(
        af::net::tcp_endpoint::host("0.0.0.0", 1234), address, error));
    EXPECT_EQ(address.family, AF_INET);
}

TEST(NetSocketAddressTests, ConvertsSocketAddressBackToEndpoint) {
    sockaddr_in6 ipv6{};
    ipv6.sin6_family = AF_INET6;
    ipv6.sin6_port = htons(23456);
    ASSERT_EQ(::inet_pton(AF_INET6, "::1", &ipv6.sin6_addr), 1);

    const af::net::tcp_endpoint endpoint = af::detail::endpoint_from_socket_address(
        reinterpret_cast<const sockaddr *>(&ipv6), sizeof(ipv6));
    EXPECT_EQ(endpoint.address, "::1");
    EXPECT_EQ(endpoint.port, 23456U);
    EXPECT_EQ(endpoint.family, af::net::address_family::ipv6);
}

TEST(NetSocketAddressTests, ConvertsUnixEndpointToSocketAddress) {
    const af::net::unix_endpoint endpoint = af::net::unix_endpoint::unix_path("/tmp/af-test.sock");
    af::detail::socket_address address{};
    int error = 0;

    ASSERT_TRUE(af::detail::socket_address_from_endpoint(endpoint, address, error));
    EXPECT_EQ(error, 0);
    EXPECT_EQ(address.family, AF_UNIX);

    const auto *unix_address = reinterpret_cast<const sockaddr_un *>(&address.storage);
    EXPECT_EQ(unix_address->sun_family, AF_UNIX);
    EXPECT_STREQ(unix_address->sun_path, "/tmp/af-test.sock");

    const af::net::unix_endpoint roundtrip = af::detail::endpoint_from_socket_address(
        reinterpret_cast<const sockaddr *>(unix_address), address.size);
    EXPECT_EQ(roundtrip.address, "/tmp/af-test.sock");
    EXPECT_EQ(roundtrip.family, af::net::address_family::unix_domain);
}
