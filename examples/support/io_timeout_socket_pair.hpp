#pragma once

#include "io_timeout_runtime.hpp"
#include "posix_socket_flags.hpp"

#include <sys/socket.h>

namespace io_timeout_example {

struct TimeoutSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (!asyncflow_examples::socket_pair_with_flags(AF_UNIX, SOCK_STREAM, 0, fds)) {
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        return true;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

} // namespace io_timeout_example
