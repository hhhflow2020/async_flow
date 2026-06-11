#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/net/detail/socket_address.hpp"
#include "af/net/tcp_connection_runtime.hpp"
#include "af/net/endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"

namespace af::net {

using tcp_client_connect_callback = void (*)(void *owner, tcp_connection_ref conn) noexcept;
using tcp_client_read_callback = void (*)(void *owner, tcp_connection_ref conn,
                                          af::buffer_view bytes) noexcept;
using tcp_client_close_callback = void (*)(void *owner, tcp_connection_ref conn,
                                           close_reason reason) noexcept;
using tcp_client_error_callback = void (*)(void *owner, int error) noexcept;

struct tcp_client_callbacks {
    void *owner{nullptr};
    tcp_client_connect_callback on_connect{nullptr};
    tcp_client_read_callback on_read{nullptr};
    tcp_client_close_callback on_close{nullptr};
    tcp_client_error_callback on_error{nullptr};
};

struct tcp_client_connect_config {
    std::string name;
    tcp_endpoint remote_endpoint;
    tcp_endpoint local_endpoint = tcp_endpoint::any(0);
    bool bind_local{false};
    af::thread_ref owner_thread{};
    tcp_connection_config connection;
    std::chrono::milliseconds connect_timeout{std::chrono::seconds(30)};
};

class tcp_client : private detail::tcp_connection_owner {
public:
    explicit tcp_client(af::runtime &owner);
    tcp_client(const tcp_client &) = delete;
    tcp_client &operator=(const tcp_client &) = delete;
    ~tcp_client();

    [[nodiscard]] bool connect(tcp_client_connect_config config,
                               tcp_client_callbacks callbacks) noexcept;
    [[nodiscard]] bool stop() noexcept;

    [[nodiscard]] bool running() const noexcept {
        return running_;
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        return connection_count_;
    }

    [[nodiscard]] std::size_t pending_connect_count() const noexcept {
        return pending_connects_.size();
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return owner_thread_;
    }

private:
    struct connection_entry;
    struct pending_connect;

    [[nodiscard]] af::runtime &runtime_owner() noexcept override;
    [[nodiscard]] send_result send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                 af::buffer buffer) noexcept override;
    [[nodiscard]] send_result send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                 af::buffer_view view) noexcept override;
    [[nodiscard]] bool pause_connection_read(std::uint32_t slot,
                                             std::uint32_t generation) noexcept override;
    [[nodiscard]] bool resume_connection_read(std::uint32_t slot,
                                              std::uint32_t generation) noexcept override;
    [[nodiscard]] bool set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                               bool enabled) noexcept override;
    [[nodiscard]] bool set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                bool enabled) noexcept override;
    [[nodiscard]] bool close_connection(std::uint32_t slot, std::uint32_t generation,
                                        close_reason reason) noexcept override;
    [[nodiscard]] bool close_connection_after_flush(std::uint32_t slot,
                                                    std::uint32_t generation) noexcept override;

    [[nodiscard]] bool on_owner_io_thread() const noexcept;
    [[nodiscard]] af::thread_ref
    current_or_configured_owner_thread(af::thread_ref configured) const noexcept;
    [[nodiscard]] bool normalize_connect_config(tcp_client_connect_config &config) const noexcept;
    [[nodiscard]] int
    validate_connect_config(const tcp_client_connect_config &config) const noexcept;
    [[nodiscard]] int open_socket(const af::detail::socket_address &remote,
                                  const af::detail::socket_address &local, bool bind_local,
                                  const tcp_client_connect_config &config) noexcept;
    [[nodiscard]] bool start_pending_connect(int fd, af::detail::socket_address remote,
                                             tcp_client_connect_config config,
                                             tcp_client_callbacks callbacks,
                                             bool already_connected = false) noexcept;
    [[nodiscard]] bool
    register_pending_connect(const std::shared_ptr<pending_connect> &pending) noexcept;
    [[nodiscard]] std::shared_ptr<pending_connect> find_pending(pending_connect *pending) noexcept;
    void complete_pending_connect(pending_connect &pending, int error) noexcept;
    [[nodiscard]] bool adopt_connected_socket(pending_connect &pending) noexcept;
    void fail_pending_connect(pending_connect &pending, int error) noexcept;
    void erase_pending_connect(pending_connect *pending) noexcept;
    void cancel_pending_connect(pending_connect &pending) noexcept;
    void cancel_all_pending_connects() noexcept;
    void refresh_running_state() noexcept;
    void begin_user_callback() noexcept;
    void end_user_callback() noexcept;
    static void on_connection_callback_begin(void *owner) noexcept;
    static void on_connection_callback_end(void *owner) noexcept;

    [[nodiscard]] connection_entry *acquire_connection_slot();
    [[nodiscard]] std::uint32_t next_connection_generation() noexcept;
    void retire_connection_slot(std::uint32_t slot, std::uint32_t generation) noexcept;
    void reap_retired_connections() noexcept;
    void reap_retired_connections_if_safe() noexcept;
    void release_connection_slot(std::uint32_t slot) noexcept;
    void close_all_connections() noexcept;
    [[nodiscard]] connection_entry *find_connection_entry(std::uint32_t slot,
                                                          std::uint32_t generation) noexcept;
    [[nodiscard]] tcp_connection *find_connection(std::uint32_t slot,
                                                  std::uint32_t generation) noexcept;
    static void on_connection_inactive(void *owner, tcp_connection &connection) noexcept;
    static void on_pending_connect_event(void *owner, af::fd_event_source &source,
                                         std::uint32_t events) noexcept;

    struct connection_entry {
        tcp_client *client{nullptr};
        std::unique_ptr<tcp_connection> connection;
        std::uint32_t slot{0};
        std::uint32_t generation{0};
        bool occupied{false};
        bool retired{false};
    };

    struct connection_slot_ref {
        std::uint32_t slot{0};
        std::uint32_t generation{0};
    };

    struct pending_connect {
        tcp_client *client{nullptr};
        af::thread_ref owner_thread{};
        int fd{-1};
        af::detail::socket_address remote{};
        tcp_client_connect_config config;
        tcp_client_callbacks callbacks{};
        tcp_endpoint local_endpoint{};
        af::fd_event_source source{};
        bool registered{false};
        bool completed{false};
    };

    af::runtime *owner_{nullptr};
    af::thread_ref owner_thread_{};
    std::shared_ptr<detail::tcp_connection_handle_state> handle_state_;
    std::vector<std::unique_ptr<connection_entry>> connections_;
    std::vector<std::uint32_t> free_connection_slots_;
    std::vector<connection_slot_ref> retired_connection_slots_;
    std::vector<std::shared_ptr<pending_connect>> pending_connects_;
    std::uint32_t next_connection_generation_{1};
    std::size_t connection_count_{0};
    std::uint32_t user_callback_depth_{0};
    bool running_{false};
};

} // namespace af::net

#include "af/net/detail/tcp_client_runtime_impl.hpp"
