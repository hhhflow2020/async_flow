#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <thread>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_pollable_client_example {

#if defined(__linux__)

inline bool read_exact_until(int fd, char* output, std::size_t size) {
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

inline bool write_exact_until(int fd, const char* input, std::size_t size) {
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

#endif

} // namespace io_pollable_client_example
