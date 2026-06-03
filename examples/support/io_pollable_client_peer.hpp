#pragma once

#include "io_pollable_client_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace io_pollable_client_example {

[[nodiscard]] inline bool apply_pollable_socket_flags(int fd) noexcept {
    if (!asyncflow_examples::apply_socket_flags(fd)) {
        return false;
    }

#if defined(SO_NOSIGPIPE)
    const int on = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on)) != 0) {
        return false;
    }
#endif
    return true;
}

struct PollableSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (!asyncflow_examples::socket_pair_with_flags(AF_UNIX, SOCK_STREAM, 0, fds)) {
            return false;
        }
        client.reset(fds[0]);
        peer.reset(fds[1]);
        if (!apply_pollable_socket_flags(client.get()) ||
            !apply_pollable_socket_flags(peer.get())) {
            return false;
        }
        return true;
    }

    af::UniqueFd client{};
    af::UniqueFd peer{};
};

inline bool read_exact_until(int fd, char *output, std::size_t size) {
    std::size_t read_bytes = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (read_bytes < size && std::chrono::steady_clock::now() < deadline) {
        const ssize_t n = ::read(fd, output + read_bytes, size - read_bytes);
        if (n > 0) {
            read_bytes += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    return read_bytes == size;
}

inline bool write_exact_until(int fd, const char *input, std::size_t size) {
    std::size_t written = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (written < size && std::chrono::steady_clock::now() < deadline) {
        const ssize_t n = ::write(fd, input + written, size - written);
        if (n > 0) {
            written += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    return written == size;
}

inline void echo_peer_once(int fd) {
    char request[4]{};
    if (read_exact_until(fd, request, sizeof(request))) {
        static_cast<void>(write_exact_until(fd, request, sizeof(request)));
    }
}

} // namespace io_pollable_client_example
