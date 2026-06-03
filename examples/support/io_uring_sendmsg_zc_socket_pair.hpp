#pragma once

#include <chrono>
#include <cstddef>
#include <cstring>
#include <thread>

#include "io_uring_sendmsg_zc_runtime.hpp"

#if defined(__linux__)
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace io_uring_sendmsg_zc_example {

#if defined(__linux__)

struct SendmsgZcSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
            return false;
        }
        sender.reset(fds[0]);
        receiver.reset(fds[1]);
        return true;
    }

    [[nodiscard]] bool read_payload_exact(char *output, std::size_t size) noexcept {
        std::size_t offset = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (offset < size) {
            const ssize_t n = ::read(receiver.get(), output + offset, size - offset);
            if (n > 0) {
                offset += static_cast<std::size_t>(n);
                continue;
            }
            if (n == 0) {
                return false;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;
            }
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    af::UniqueFd sender{};
    af::UniqueFd receiver{};
};

#else

struct SendmsgZcSocketPair {
    [[nodiscard]] bool create() noexcept {
        return false;
    }

    [[nodiscard]] bool read_payload_exact(char *output, std::size_t size) noexcept {
        static_cast<void>(output);
        static_cast<void>(size);
        return false;
    }

    af::UniqueFd sender{};
    af::UniqueFd receiver{};
};

#endif

[[nodiscard]] inline bool payload_matches(const char *received) noexcept {
    return std::memcmp(received, sendmsg_zc_first, sendmsg_zc_first_size) == 0 &&
           std::memcmp(received + sendmsg_zc_first_size, sendmsg_zc_second,
                       sendmsg_zc_second_size) == 0;
}

} // namespace io_uring_sendmsg_zc_example
