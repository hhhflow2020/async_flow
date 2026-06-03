#pragma once

#include <cstddef>

#include "../app_runtime.hpp"
#include "posix_socket_flags.hpp"

#if defined(__linux__)
#include <cerrno>
#include <chrono>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#endif

namespace io_epoll_example {

#if defined(__linux__)

struct EpollSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (!asyncflow_examples::socket_pair_with_flags(AF_UNIX, SOCK_STREAM, 0, fds)) {
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        return true;
    }

    [[nodiscard]] bool write_byte(char value) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        for (;;) {
            const ssize_t written = ::write(writer.get(), &value, sizeof(value));
            if (written == static_cast<ssize_t>(sizeof(value))) {
                return true;
            }
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
                std::chrono::steady_clock::now() <= deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            return false;
        }
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

#else

struct EpollSocketPair {
    [[nodiscard]] bool create() noexcept {
        return false;
    }

    [[nodiscard]] bool write_byte(char value) noexcept {
        static_cast<void>(value);
        return false;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

#endif

} // namespace io_epoll_example
