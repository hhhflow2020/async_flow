#pragma once

#include "io_shutdown_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <cerrno>
#include <chrono>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace io_shutdown_example {

struct ShutdownSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (!asyncflow_examples::socket_pair_with_flags(AF_UNIX, SOCK_STREAM, 0, fds)) {
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

} // namespace io_shutdown_example
