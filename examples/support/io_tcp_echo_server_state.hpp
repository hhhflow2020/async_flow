#pragma once

#include <atomic>
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

[[nodiscard]] inline std::uint64_t echo_session_started(EchoServerState &state) noexcept {
    return state.active_sessions.fetch_add(1, std::memory_order_relaxed) + 1U;
}

inline void echo_session_start_aborted(EchoServerState &state) noexcept {
    if (state.active_sessions.fetch_sub(1, std::memory_order_release) == 1U) {
        state.active_sessions.notify_all();
    }
}

inline void echo_session_finished(EchoServerState &state, bool success,
                                  std::uint64_t bytes_received, std::uint64_t bytes_sent) noexcept {
    state.bytes_received.fetch_add(bytes_received, std::memory_order_relaxed);
    state.bytes_sent.fetch_add(bytes_sent, std::memory_order_relaxed);
    if (success) {
        state.completed_sessions.fetch_add(1, std::memory_order_relaxed);
    } else {
        state.failed_sessions.fetch_add(1, std::memory_order_relaxed);
    }

    if (state.active_sessions.fetch_sub(1, std::memory_order_release) == 1U) {
        state.active_sessions.notify_all();
    }
}

inline void echo_accept_finished(EchoServerState &state, int error) noexcept {
    state.accept_error.store(error, std::memory_order_release);
    state.accept_stopped.store(true, std::memory_order_release);
    state.accept_stopped.notify_all();
}

} // namespace io_tcp_echo_example
