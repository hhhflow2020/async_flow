#pragma once

#include <cerrno>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/udp_types.hpp"

namespace af::net::detail {

template <typename Runtime> class UdpHandlerBase {
public:
    virtual ~UdpHandlerBase() = default;
    [[nodiscard]] virtual std::unique_ptr<UdpHandlerBase> clone() const = 0;
    virtual void on_datagram(UdpSocketRef<Runtime> socket, af::BufferView bytes,
                             const UdpPeer &peer) noexcept = 0;
    virtual void on_error(UdpSocketHandle<Runtime> socket, int error) noexcept = 0;
};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagramPeer : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagramPeer<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>(),
        std::declval<const UdpPeer &>()))>> : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagramEndpoint : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagramEndpoint<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>(),
        std::declval<const UdpEndpoint &>()))>> : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagram : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagram<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>()))>> : std::true_type {
};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnError : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnError<Handler, Runtime,
                            std::void_t<decltype(std::declval<Handler &>().on_error(
                                std::declval<UdpSocketHandle<Runtime>>(), std::declval<int>()))>>
    : std::true_type {};

template <typename Runtime, typename Handler>
class UdpHandlerModel final : public UdpHandlerBase<Runtime> {
public:
    explicit UdpHandlerModel(Handler handler) : handler_(std::move(handler)) {}

    [[nodiscard]] std::unique_ptr<UdpHandlerBase<Runtime>> clone() const override {
        return std::make_unique<UdpHandlerModel>(handler_);
    }

    void on_datagram(UdpSocketRef<Runtime> socket, af::BufferView bytes,
                     const UdpPeer &peer) noexcept override {
        if constexpr (UdpHandlerHasOnDatagramPeer<Handler, Runtime>::value) {
            try {
                handler_.on_datagram(socket, bytes, peer);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else if constexpr (UdpHandlerHasOnDatagramEndpoint<Handler, Runtime>::value) {
            try {
                const UdpEndpoint endpoint = peer.endpoint();
                handler_.on_datagram(socket, bytes, endpoint);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else if constexpr (UdpHandlerHasOnDatagram<Handler, Runtime>::value) {
            try {
                handler_.on_datagram(socket, bytes);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else {
            static_cast<void>(socket);
            static_cast<void>(bytes);
            static_cast<void>(peer);
        }
    }

    void on_error(UdpSocketHandle<Runtime> socket, int error) noexcept override {
        if constexpr (UdpHandlerHasOnError<Handler, Runtime>::value) {
            try {
                handler_.on_error(socket, error);
            } catch (...) {
            }
        } else {
            static_cast<void>(socket);
            static_cast<void>(error);
        }
    }

private:
    Handler handler_;
};

template <typename Runtime> struct UdpSocketContext {
    std::string name;
    UdpEndpoint local_endpoint;
    UdpEndpoint remote_endpoint;
    UdpSocketOptions options;
    bool connect_remote{false};
    std::unique_ptr<UdpHandlerBase<Runtime>> handler;
};

} // namespace af::net::detail
