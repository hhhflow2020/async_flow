#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>

#include "io_tcp_echo_runtime.hpp"

#if !defined(_WIN32)
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#endif

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
    int shutdown_notify_fd{-1};
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

[[nodiscard]] inline bool echo_server_drained(const EchoServerState &state) noexcept {
    return state.accept_stopped.load(std::memory_order_acquire) &&
           state.active_sessions.load(std::memory_order_acquire) == 0U;
}

#if !defined(_WIN32)
[[nodiscard]] inline bool echo_set_notify_fd_flags(int fd) noexcept {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return false;
    }

    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    return descriptor_flags >= 0 && ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0;
}

struct EchoShutdownNotifier {
    af::UniqueFd read_fd{};
    af::UniqueFd write_fd{};
    int error{0};

    [[nodiscard]] bool open() noexcept {
        error = 0;
        int fds[2]{-1, -1};
#if defined(__linux__) && defined(O_NONBLOCK) && defined(O_CLOEXEC)
        if (::pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
#else
        if (::pipe(fds) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
        af::UniqueFd read(fds[0]);
        af::UniqueFd write(fds[1]);
        if (!echo_set_notify_fd_flags(read.get()) || !echo_set_notify_fd_flags(write.get())) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
        read_fd = std::move(read);
        write_fd = std::move(write);
        return true;
#endif

        read_fd.reset(fds[0]);
        write_fd.reset(fds[1]);
        return true;
    }
};

inline void echo_notify_shutdown_state(EchoServerState &state) noexcept {
    if (state.shutdown_notify_fd < 0) {
        return;
    }

    const char byte = 0;
    for (;;) {
        const auto written = ::write(state.shutdown_notify_fd, &byte, 1);
        if (written == 1) {
            return;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return;
    }
}

inline void echo_drain_shutdown_notifications(int fd) noexcept {
    if (fd < 0) {
        return;
    }

    char buffer[64]{};
    for (;;) {
        const auto read = ::read(fd, buffer, sizeof(buffer));
        if (read > 0) {
            continue;
        }
        if (read == 0) {
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        return;
    }
}

[[nodiscard]] inline int
echo_shutdown_wait_timeout_ms(std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        return 0;
    }

    const auto remaining = deadline - now;
    const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    if (remaining_ms.count() <= 0) {
        return 1;
    }
    if (remaining_ms.count() > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(remaining_ms.count());
}

[[nodiscard]] inline bool
echo_wait_for_shutdown(EchoServerState &state, int notify_fd,
                       std::chrono::steady_clock::time_point deadline) noexcept {
    if (echo_server_drained(state)) {
        return true;
    }
    if (notify_fd < 0) {
        return false;
    }

    echo_drain_shutdown_notifications(notify_fd);
    pollfd descriptor{notify_fd, POLLIN, 0};
    while (std::chrono::steady_clock::now() < deadline) {
        if (echo_server_drained(state)) {
            return true;
        }

        descriptor.revents = 0;
        const int ready = ::poll(&descriptor, 1, echo_shutdown_wait_timeout_ms(deadline));
        if (ready > 0) {
            echo_drain_shutdown_notifications(notify_fd);
            continue;
        }
        if (ready == 0) {
            break;
        }
        if (errno != EINTR) {
            break;
        }
    }
    return echo_server_drained(state);
}
#else
inline void echo_notify_shutdown_state(EchoServerState &state) noexcept {
    static_cast<void>(state);
}

[[nodiscard]] inline bool
echo_wait_for_shutdown(EchoServerState &state, int notify_fd,
                       std::chrono::steady_clock::time_point deadline) noexcept {
    static_cast<void>(notify_fd);
    static_cast<void>(deadline);
    return echo_server_drained(state);
}
#endif

[[nodiscard]] inline std::uint64_t echo_session_started(EchoServerState &state) noexcept {
    return state.active_sessions.fetch_add(1, std::memory_order_relaxed) + 1U;
}

inline void echo_session_start_aborted(EchoServerState &state) noexcept {
    if (state.active_sessions.fetch_sub(1, std::memory_order_release) == 1U) {
        state.active_sessions.notify_all();
        echo_notify_shutdown_state(state);
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
        echo_notify_shutdown_state(state);
    }
}

inline void echo_accept_finished(EchoServerState &state, int error) noexcept {
    state.accept_error.store(error, std::memory_order_release);
    state.accept_stopped.store(true, std::memory_order_release);
    state.accept_stopped.notify_all();
    echo_notify_shutdown_state(state);
}

} // namespace io_tcp_echo_example
