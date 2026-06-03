#pragma once

#include "io_shutdown_runtime.hpp"

#if defined(__linux__)
#include <cerrno>
#include <chrono>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#endif

namespace io_shutdown_example {

#if defined(__linux__)

struct ShutdownSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
            return false;
        }
        local.reset(fds[0]);
        peer.reset(fds[1]);
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

#else

struct ShutdownSocketPair {
    [[nodiscard]] bool create() noexcept {
        return false;
    }

    [[nodiscard]] bool peer_observed_eof() noexcept {
        return false;
    }

    af::UniqueFd local{};
    af::UniqueFd peer{};
};

#endif

} // namespace io_shutdown_example
