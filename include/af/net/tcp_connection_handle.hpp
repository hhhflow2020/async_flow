#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"

namespace af::net {

class tcp_connection;
class tcp_connection_handle;
class tcp_connection_ref;

using tcp_connection_accept_callback = void (*)(void *owner, tcp_connection_ref conn) noexcept;
using tcp_connection_read_callback = void (*)(void *owner, tcp_connection_ref conn,
                                              af::BufferView bytes) noexcept;
using tcp_connection_close_callback = void (*)(void *owner, tcp_connection_ref conn,
                                               close_reason reason) noexcept;

struct tcp_connection_callbacks {
    void *owner{nullptr};
    tcp_connection_accept_callback on_accept{nullptr};
    tcp_connection_read_callback on_read{nullptr};
    tcp_connection_close_callback on_close{nullptr};
};

namespace detail {

class tcp_connection_owner {
public:
    [[nodiscard]] virtual af::runtime &runtime_owner() noexcept = 0;
    [[nodiscard]] virtual send_result send_to_connection(std::uint32_t slot,
                                                         std::uint32_t generation,
                                                         af::Buffer buffer) noexcept = 0;
    [[nodiscard]] virtual send_result send_to_connection(std::uint32_t slot,
                                                         std::uint32_t generation,
                                                         af::BufferView view) noexcept = 0;
    [[nodiscard]] virtual bool pause_connection_read(std::uint32_t slot,
                                                     std::uint32_t generation) noexcept = 0;
    [[nodiscard]] virtual bool resume_connection_read(std::uint32_t slot,
                                                      std::uint32_t generation) noexcept = 0;
    [[nodiscard]] virtual bool set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                                       bool enabled) noexcept = 0;
    [[nodiscard]] virtual bool set_connection_keepalive(std::uint32_t slot,
                                                        std::uint32_t generation,
                                                        bool enabled) noexcept = 0;
    [[nodiscard]] virtual bool close_connection(std::uint32_t slot, std::uint32_t generation,
                                                close_reason reason) noexcept = 0;
    [[nodiscard]] virtual bool close_connection_after_flush(std::uint32_t slot,
                                                            std::uint32_t generation) noexcept = 0;

protected:
    ~tcp_connection_owner() = default;
};

using tcp_connection_inactive_callback = void (*)(void *owner, tcp_connection &connection) noexcept;
using tcp_connection_lifecycle_callback = void (*)(void *owner) noexcept;

struct tcp_connection_lifecycle {
    void *owner{nullptr};
    tcp_connection_inactive_callback on_inactive{nullptr};
    tcp_connection_lifecycle_callback on_callback_begin{nullptr};
    tcp_connection_lifecycle_callback on_callback_end{nullptr};
};

struct tcp_connection_handle_state {
    std::atomic<tcp_connection_owner *> owner{nullptr};
    std::atomic<bool> accepting_operations{false};
};

} // namespace detail

class tcp_connection_handle {
public:
    tcp_connection_handle() noexcept = default;

    [[nodiscard]] bool valid() const noexcept {
        return owner_thread_.valid() && generation_ != 0U;
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return owner_thread_;
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return slot_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] send_result send(af::Buffer buffer) const noexcept;
    [[nodiscard]] send_result send(af::BufferView view) const noexcept;
    [[nodiscard]] bool pause_read() const noexcept;
    [[nodiscard]] bool resume_read() const noexcept;
    [[nodiscard]] bool set_no_delay(bool enabled) const noexcept;
    [[nodiscard]] bool set_keepalive(bool enabled) const noexcept;
    [[nodiscard]] bool close(close_reason reason = close_reason::local) const noexcept;
    [[nodiscard]] bool close_now(close_reason reason = close_reason::local) const noexcept;
    [[nodiscard]] bool close_after_flush() const noexcept;

    [[nodiscard]] friend bool operator==(tcp_connection_handle lhs,
                                         tcp_connection_handle rhs) noexcept {
        return lhs.owner_thread_ == rhs.owner_thread_ && lhs.slot_ == rhs.slot_ &&
               lhs.generation_ == rhs.generation_;
    }

    [[nodiscard]] friend bool operator!=(tcp_connection_handle lhs,
                                         tcp_connection_handle rhs) noexcept {
        return !(lhs == rhs);
    }

private:
    friend class tcp_connection;

    tcp_connection_handle(std::weak_ptr<detail::tcp_connection_handle_state> state,
                          af::thread_ref owner_thread, std::uint32_t slot,
                          std::uint32_t generation) noexcept
        : state_(std::move(state)), owner_thread_(owner_thread), slot_(slot),
          generation_(generation) {}

    std::weak_ptr<detail::tcp_connection_handle_state> state_;
    af::thread_ref owner_thread_{};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
};

class tcp_connection_ref {
public:
    explicit tcp_connection_ref(tcp_connection *connection = nullptr) noexcept
        : connection_(connection) {}

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] tcp_connection_handle handle() const noexcept;
    [[nodiscard]] af::thread_ref owner_thread() const noexcept;
    [[nodiscard]] std::uint32_t slot() const noexcept;
    [[nodiscard]] std::uint32_t generation() const noexcept;
    [[nodiscard]] const tcp_endpoint &local_endpoint() const noexcept;
    [[nodiscard]] const tcp_endpoint &peer_endpoint() const noexcept;
    [[nodiscard]] std::size_t queued_bytes() const noexcept;
    [[nodiscard]] send_result send(af::Buffer buffer) const noexcept;
    [[nodiscard]] send_result send(af::BufferView view) const noexcept;
    [[nodiscard]] bool pause_read() const noexcept;
    [[nodiscard]] bool resume_read() const noexcept;
    [[nodiscard]] bool set_no_delay(bool enabled) const noexcept;
    [[nodiscard]] bool set_keepalive(bool enabled) const noexcept;
    void close(close_reason reason = close_reason::local) const noexcept;
    void close_after_flush() const noexcept;

private:
    tcp_connection *connection_{nullptr};
};

} // namespace af::net
