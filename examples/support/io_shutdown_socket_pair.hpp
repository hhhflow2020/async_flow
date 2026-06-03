#pragma once

#include "io_shutdown_runtime.hpp"

#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace io_shutdown_example {

[[nodiscard]] inline bool apply_shutdown_socket_flags(int fd) noexcept {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return false;
    }

    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return false;
    }
    return true;
}

struct ShutdownSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            return false;
        }
        local.reset(fds[0]);
        peer.reset(fds[1]);
        if (!apply_shutdown_socket_flags(local.get()) || !apply_shutdown_socket_flags(peer.get())) {
            return false;
        }
        return true;
    }

    [[nodiscard]] bool peer_observed_eof() noexcept {
        char ignored = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        for (;;) {
            const ssize_t read_result = ::read(peer.get(), &ignored, sizeof(ignored));
            if (read_result == 0) {
                return true;
            }
            if (read_result < 0 && errno == EINTR) {
                continue;
            }
            if (read_result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
                std::chrono::steady_clock::now() <= deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            return false;
        }
    }

    af::UniqueFd local{};
    af::UniqueFd peer{};
};

} // namespace io_shutdown_example
