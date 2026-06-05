#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_endpoint.hpp"

#include <sys/socket.h>

namespace af::net {

template <typename Runtime> class UdpSocketHandle;
template <typename Runtime> class UdpSocketRef;

namespace detail {
template <typename Runtime> class UdpSocketShard;
} // namespace detail

enum class UdpSendResult : std::uint8_t {
    Accepted,
    Queued,
    Backpressure,
    Closed,
    Unsupported,
    accepted = Accepted,
    queued = Queued,
    backpressure = Backpressure,
    closed = Closed,
    unsupported = Unsupported,
};

struct UdpSocketOptions {
    bool reuse_port{true};
    bool ipv6_only{true};
    std::size_t read_budget_datagrams{64};
    std::size_t receive_buffer_size{64U * 1024U};
    std::size_t max_datagram_size{64U * 1024U};
    bool unlink_existing_unix_path{true};
    bool unlink_unix_path_on_close{true};
};

struct UdpSocketRuntimeConfig {};

class UdpPeer {
public:
    UdpPeer() = default;

    UdpPeer(const sockaddr *address, socklen_t size) noexcept {
        assign(address, size);
    }

    [[nodiscard]] bool valid() const noexcept {
        return address_.size != 0U;
    }

    [[nodiscard]] int native_family() const noexcept {
        return address_.family;
    }

    [[nodiscard]] const sockaddr *native_address() const noexcept {
        return reinterpret_cast<const sockaddr *>(&address_.storage);
    }

    [[nodiscard]] socklen_t native_address_size() const noexcept {
        return address_.size;
    }

    [[nodiscard]] UdpEndpoint endpoint() const {
        return af::detail::endpoint_from_socket_address(native_address(), address_.size);
    }

private:
    template <typename Runtime> friend class UdpSocketHandle;
    template <typename Runtime> friend class UdpSocketRef;
    template <typename Runtime> friend class detail::UdpSocketShard;

    void assign(const sockaddr *address, socklen_t size) noexcept {
        address_ = af::detail::SocketAddress{};
        if (address == nullptr || size == 0U || size > sizeof(address_.storage)) {
            return;
        }
        std::memcpy(&address_.storage, address, size);
        address_.size = size;
        address_.family = address->sa_family;
    }

    [[nodiscard]] const af::detail::SocketAddress &socket_address() const noexcept {
        return address_;
    }

    af::detail::SocketAddress address_{};
};

using udp_send_result = UdpSendResult;
using udp_socket_options = UdpSocketOptions;
using udp_socket_runtime_config = UdpSocketRuntimeConfig;
using udp_peer = UdpPeer;

} // namespace af::net
