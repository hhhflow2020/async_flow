#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_endpoint.hpp"

#include <sys/socket.h>

namespace af::net {

class udp_socket_handle;
class udp_socket_ref;

namespace detail {
struct runtime_udp_shard;
} // namespace detail

enum class udp_send_result : std::uint8_t {
    accepted,
    queued,
    backpressure,
    closed,
    unsupported,
    Accepted = accepted,
    Queued = queued,
    Backpressure = backpressure,
    Closed = closed,
    Unsupported = unsupported,
};

struct udp_socket_options {
    bool reuse_port{true};
    bool ipv6_only{true};
    std::size_t read_budget_datagrams{64};
    std::size_t receive_buffer_size{64U * 1024U};
    std::size_t max_datagram_size{64U * 1024U};
    bool unlink_existing_unix_path{true};
    bool unlink_unix_path_on_close{true};
};

struct udp_socket_runtime_config {};

class udp_peer {
public:
    udp_peer() = default;

    udp_peer(const sockaddr *address, socklen_t size) noexcept {
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

    [[nodiscard]] udp_endpoint endpoint() const {
        return af::detail::endpoint_from_socket_address(native_address(), address_.size);
    }

private:
    friend class udp_socket_handle;
    friend class udp_socket_ref;
    friend struct detail::runtime_udp_shard;

    void assign(const sockaddr *address, socklen_t size) noexcept {
        address_ = af::detail::socket_address{};
        if (address == nullptr || size == 0U || size > sizeof(address_.storage)) {
            return;
        }
        std::memcpy(&address_.storage, address, size);
        address_.size = size;
        address_.family = address->sa_family;
    }

    [[nodiscard]] const af::detail::socket_address &socket_address() const noexcept {
        return address_;
    }

    af::detail::socket_address address_{};
};

using UdpSendResult = udp_send_result;
using UdpSocketOptions = udp_socket_options;
using UdpSocketRuntimeConfig = udp_socket_runtime_config;
using UdpPeer = udp_peer;

} // namespace af::net
