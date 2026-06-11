#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "af/net/tcp_endpoint.hpp"
#include "af/net/thread_list.hpp"
#include "af/runtime_config.hpp"

namespace af::net {

enum class send_result : std::uint8_t {
    accepted,
    queued,
    backpressure,
    closed,
    unsupported,
};

enum class close_reason : std::uint8_t {
    local,
    peer,
    error,
};

enum class tcp_accept_strategy : std::uint8_t {
    auto_select,
    reuse_port_per_io_thread,
    single_acceptor,
};

enum class listener_state : std::uint8_t {
    configured,
    starting,
    active,
    failed,
    removed,
};

enum class remove_listener_policy : std::uint8_t {
    stop_accept_only,
    close_existing_connections,
};

struct tcp_listener_options {
    int backlog{4096};
    bool reuse_port{true};
    bool ipv6_only{true};
    std::size_t accept_budget{128};
    std::size_t read_budget_bytes{256U * 1024U};
    std::size_t read_buffer_size{16U * 1024U};
    std::size_t write_budget_bytes{512U * 1024U};
    std::size_t output_high_watermark{4U * 1024U * 1024U};
    bool no_delay{true};
    bool keepalive{true};
    bool unlink_existing_unix_path{true};
    bool unlink_unix_path_on_close{true};
};

struct tcp_connection_config {
    std::size_t read_buffer_size{16U * 1024U};
    std::size_t read_budget_bytes{512U * 1024U};
    std::size_t write_budget_bytes{512U * 1024U};
    std::size_t output_high_watermark{8U * 1024U * 1024U};
    bool no_delay{true};
    bool keepalive{true};
};

struct tcp_server_config {
    tcp_connection_config connection;
    std::chrono::milliseconds connection_close_timeout{std::chrono::seconds(5)};
};

struct tcp_listener_config {
    std::string name;
    tcp_endpoint endpoint;
    std::vector<af::thread_ref> threads;
    tcp_listener_options options;
    tcp_accept_strategy accept_strategy{tcp_accept_strategy::auto_select};
};

struct listener_id {
    std::uint32_t slot{0};
    std::uint32_t generation{0};

    [[nodiscard]] bool valid() const noexcept {
        return generation != 0U;
    }

    [[nodiscard]] friend bool operator==(listener_id lhs, listener_id rhs) noexcept {
        return lhs.slot == rhs.slot && lhs.generation == rhs.generation;
    }

    [[nodiscard]] friend bool operator!=(listener_id lhs, listener_id rhs) noexcept {
        return !(lhs == rhs);
    }
};

struct tcp_listener_handle {
    listener_id id{};

    [[nodiscard]] bool valid() const noexcept {
        return id.valid();
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return id.slot;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return id.generation;
    }

    [[nodiscard]] friend bool operator==(tcp_listener_handle lhs,
                                         tcp_listener_handle rhs) noexcept {
        return lhs.id == rhs.id;
    }
};

struct listener_result {
    tcp_listener_handle listener{};
    int error{0};

    [[nodiscard]] bool ok() const noexcept {
        return error == 0 && listener.valid();
    }

    [[nodiscard]] static listener_result success(tcp_listener_handle handle) noexcept {
        return listener_result{handle, 0};
    }

    [[nodiscard]] static listener_result failure(int err) noexcept {
        return listener_result{tcp_listener_handle{}, err == 0 ? EINVAL : err};
    }
};

} // namespace af::net
