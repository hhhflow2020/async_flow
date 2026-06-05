#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/tcp_types.hpp"

namespace af::net::detail {

template <typename Handler, typename Runtime, typename = void>
struct TcpHandlerHasOnAccept : std::false_type {};
template <typename Handler, typename Runtime>
struct TcpHandlerHasOnAccept<Handler, Runtime,
                             std::void_t<decltype(std::declval<Handler &>().on_accept(
                                 std::declval<TcpConnectionRef<Runtime>>()))>> : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct TcpHandlerHasOnRead : std::false_type {};
template <typename Handler, typename Runtime>
struct TcpHandlerHasOnRead<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_read(
        std::declval<TcpConnectionRef<Runtime>>(), std::declval<af::BufferView>()))>>
    : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct TcpHandlerHasOnClose : std::false_type {};
template <typename Handler, typename Runtime>
struct TcpHandlerHasOnClose<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_close(
        std::declval<TcpConnectionHandle<Runtime>>(), std::declval<CloseReason>()))>>
    : std::true_type {};

template <typename Handler, typename = void>
struct TcpHandlerHasOnListenerErrorAlias : std::false_type {};
template <typename Handler>
struct TcpHandlerHasOnListenerErrorAlias<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_error(
                 std::declval<TcpListenerHandle>(), std::declval<int>()))>> : std::true_type {};

template <typename Handler, typename = void>
struct TcpHandlerHasOnListenerError : std::false_type {};
template <typename Handler>
struct TcpHandlerHasOnListenerError<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_listener_error(
                 std::declval<TcpListenerHandle>(), std::declval<int>()))>> : std::true_type {};

template <typename Runtime> class TcpHandlerBase {
public:
    virtual ~TcpHandlerBase() = default;
    [[nodiscard]] virtual std::unique_ptr<TcpHandlerBase> clone() const = 0;
    virtual void on_accept(TcpConnectionRef<Runtime> conn) noexcept = 0;
    virtual void on_read(TcpConnectionRef<Runtime> conn, af::BufferView bytes) noexcept = 0;
    virtual void on_close(TcpConnectionHandle<Runtime> conn, CloseReason reason) noexcept = 0;
    virtual void on_listener_error(TcpListenerHandle listener, int error) noexcept = 0;
};

template <typename Runtime, typename Handler>
class TcpHandlerModel final : public TcpHandlerBase<Runtime> {
public:
    explicit TcpHandlerModel(Handler handler) : handler_(std::move(handler)) {}

    [[nodiscard]] std::unique_ptr<TcpHandlerBase<Runtime>> clone() const override {
        return std::make_unique<TcpHandlerModel>(handler_);
    }

    void on_accept(TcpConnectionRef<Runtime> conn) noexcept override {
        if constexpr (TcpHandlerHasOnAccept<Handler, Runtime>::value) {
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
        if constexpr (TcpHandlerHasOnListenerErrorAlias<Handler>::value) {
            try {
                handler_.on_error(listener, error);
            } catch (...) {
            }
        } else if constexpr (TcpHandlerHasOnListenerError<Handler>::value) {
            try {
                handler_.on_listener_error(listener, error);
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
