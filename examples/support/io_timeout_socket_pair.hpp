#pragma once

#include "../app_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#endif

namespace io_timeout_example {

#if defined(__linux__)

struct TimeoutSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        return true;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

#else

struct TimeoutSocketPair {
    [[nodiscard]] bool create() noexcept {
        return false;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

#endif

} // namespace io_timeout_example
