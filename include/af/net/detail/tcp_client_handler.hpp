#pragma once

#include <cerrno>
#include <memory>
#include <type_traits>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/tcp_connection.hpp"
#include "af/net/tcp_types.hpp"

namespace af::net::detail {

template <typename Handler, typename Runtime, typename = void>
struct TcpClientHandlerHasOnConnect : std::false_type {};
template <typename Handler, typename Runtime>
struct TcpClientHandlerHasOnConnect<Handler, Runtime,
                                    std::void_t<decltype(std::declval<Handler &>().on_connect(
                                        std::declval<TcpConnectionRef<Runtime>>()))>>
    : std::true_type {};

template <typename Handler, typename = void>
struct TcpClientHandlerHasOnConnectError : std::false_type {};
template <typename Handler>
struct TcpClientHandlerHasOnConnectError<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_connect_error(std::declval<int>()))>>
    : std::true_type {};

template <typename Handler, typename = void> struct TcpClientHandlerHasOnError : std::false_type {};
template <typename Handler>
struct TcpClientHandlerHasOnError<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_error(std::declval<int>()))>>
    : std::true_type {};

template <typename Runtime, typename Handler>
class TcpClientHandlerModel final : public TcpHandlerBase<Runtime> {
public:
    explicit TcpClientHandlerModel(Handler handler) : handler_(std::move(handler)) {}

    [[nodiscard]] std::unique_ptr<TcpHandlerBase<Runtime>> clone() const override {
        return std::make_unique<TcpClientHandlerModel>(handler_);
    }

    void on_accept(TcpConnectionRef<Runtime> conn) noexcept override {
        if constexpr (TcpClientHandlerHasOnConnect<Handler, Runtime>::value) {
            try {
                handler_.on_connect(conn);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else if constexpr (TcpHandlerHasOnAccept<Handler, Runtime>::value) {
            try {
                handler_.on_accept(conn);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else {
            static_cast<void>(conn);
        }
    }

    void on_read(TcpConnectionRef<Runtime> conn, af::BufferView bytes) noexcept override {
        if constexpr (TcpHandlerHasOnRead<Handler, Runtime>::value) {
            try {
                handler_.on_read(conn, bytes);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else {
            static_cast<void>(conn);
            static_cast<void>(bytes);
        }
    }

    void on_close(TcpConnectionHandle<Runtime> conn, CloseReason reason) noexcept override {
        if constexpr (TcpHandlerHasOnClose<Handler, Runtime>::value) {
            try {
                handler_.on_close(conn, reason);
            } catch (...) {
            }
        } else {
            static_cast<void>(conn);
            static_cast<void>(reason);
        }
    }

    void on_listener_error(TcpListenerHandle listener, int error) noexcept override {
        if constexpr (TcpClientHandlerHasOnConnectError<Handler>::value) {
            try {
                handler_.on_connect_error(error);
            } catch (...) {
            }
        } else if constexpr (TcpClientHandlerHasOnError<Handler>::value) {
            try {
                handler_.on_error(error);
            } catch (...) {
            }
        } else if constexpr (TcpHandlerHasOnListenerErrorAlias<Handler>::value) {
            try {
                handler_.on_error(listener, error);
            } catch (...) {
            }
        } else {
            static_cast<void>(listener);
            static_cast<void>(error);
        }
    }

private:
    Handler handler_;
};

} // namespace af::net::detail
