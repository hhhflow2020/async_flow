#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace af::net {

enum class SendResult : std::uint8_t {
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

enum class CloseReason : std::uint8_t {
    Local,
    Peer,
    Error,
    local = Local,
    peer = Peer,
    error = Error,
};

enum class AcceptStrategy : std::uint8_t {
    Auto,
    ReusePortPerIoThread,
    SingleAcceptor,
    auto_select = Auto,
    reuse_port_per_io_thread = ReusePortPerIoThread,
    single_acceptor = SingleAcceptor,
};

enum class ListenerState : std::uint8_t {
    Configured,
    Starting,
    Active,
    Failed,
    Removed,
    configured = Configured,
    starting = Starting,
    active = Active,
    failed = Failed,
    removed = Removed,
};

enum class RemoveListenerPolicy : std::uint8_t {
    StopAcceptOnly,
    CloseExistingConnections,
    stop_accept_only = StopAcceptOnly,
    close_existing_connections = CloseExistingConnections,
};

struct TcpListenerOptions {
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

struct TcpConnectionConfig {
    std::size_t read_buffer_size{16U * 1024U};
    std::size_t read_budget_bytes{512U * 1024U};
    std::size_t write_budget_bytes{512U * 1024U};
    std::size_t output_high_watermark{8U * 1024U * 1024U};
    bool no_delay{true};
    bool keepalive{true};
};

struct TcpServerConfig {
    TcpConnectionConfig connection;
    std::chrono::milliseconds connection_close_timeout{std::chrono::seconds(5)};
};

struct ListenerId {
    std::uint32_t slot{0};
    std::uint32_t generation{0};

    [[nodiscard]] bool valid() const noexcept {
        return generation != 0U;
    }

    [[nodiscard]] friend bool operator==(ListenerId lhs, ListenerId rhs) noexcept {
        return lhs.slot == rhs.slot && lhs.generation == rhs.generation;
    }

    [[nodiscard]] friend bool operator!=(ListenerId lhs, ListenerId rhs) noexcept {
        return !(lhs == rhs);
    }
};

struct TcpListenerHandle {
    ListenerId id{};

    [[nodiscard]] bool valid() const noexcept {
        return id.valid();
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return id.slot;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return id.generation;
    }

    [[nodiscard]] friend bool operator==(TcpListenerHandle lhs, TcpListenerHandle rhs) noexcept {
        return lhs.id == rhs.id;
    }
};

struct ListenerResult {
    TcpListenerHandle listener{};
    int error{0};

    [[nodiscard]] bool ok() const noexcept {
        return error == 0 && listener.valid();
    }

    [[nodiscard]] static ListenerResult success(TcpListenerHandle handle) noexcept {
        return ListenerResult{handle, 0};
    }

    [[nodiscard]] static ListenerResult failure(int err) noexcept {
        return ListenerResult{TcpListenerHandle{}, err == 0 ? EINVAL : err};
    }
};

template <typename Runtime> class TcpConnectionHandle;
template <typename Runtime> class TcpConnectionRef;

template <typename Runtime, typename Group>
[[nodiscard]] std::vector<typename Runtime::Thread> thread_list(Group);

} // namespace af::net
