#pragma once

#include <cstdint>

#include "io_tcp_echo_runtime.hpp"

namespace io_tcp_echo_example {

struct EchoServerState {
    af::detail::CacheLineAtomic<bool> stop_requested{false};
    af::detail::CacheLineAtomic<bool> accept_stopped{false};
    af::detail::CacheLineAtomic<std::uint64_t> accepted{0};
    af::detail::CacheLineAtomic<std::uint64_t> rejected{0};
    af::detail::CacheLineAtomic<std::uint64_t> active_sessions{0};
    af::detail::CacheLineAtomic<std::uint64_t> completed_sessions{0};
    af::detail::CacheLineAtomic<std::uint64_t> failed_sessions{0};
    af::detail::CacheLineAtomic<std::uint64_t> bytes_received{0};
    af::detail::CacheLineAtomic<std::uint64_t> bytes_sent{0};
    af::detail::CacheLineAtomic<int> accept_error{0};
};

struct EchoServerSnapshot {
    std::uint64_t accepted{0};
    std::uint64_t rejected{0};
    std::uint64_t active_sessions{0};
    std::uint64_t completed_sessions{0};
    std::uint64_t failed_sessions{0};
    std::uint64_t bytes_received{0};
    std::uint64_t bytes_sent{0};
    int accept_error{0};
};

[[nodiscard]] inline EchoServerSnapshot
echo_server_snapshot(const EchoServerState &state) noexcept {
    return {
        .accepted = state.accepted.load(std::memory_order_acquire),
        .rejected = state.rejected.load(std::memory_order_acquire),
        .active_sessions = state.active_sessions.load(std::memory_order_acquire),
        .completed_sessions = state.completed_sessions.load(std::memory_order_acquire),
        .failed_sessions = state.failed_sessions.load(std::memory_order_acquire),
        .bytes_received = state.bytes_received.load(std::memory_order_acquire),
        .bytes_sent = state.bytes_sent.load(std::memory_order_acquire),
        .accept_error = state.accept_error.load(std::memory_order_acquire),
    };
}

} // namespace io_tcp_echo_example
