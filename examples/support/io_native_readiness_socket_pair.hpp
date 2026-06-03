#pragma once

#include "af/async_flow.hpp"
#include "posix_socket_flags.hpp"

#include <sys/socket.h>
#include <unistd.h>

namespace io_native_readiness_example {

struct NativeReadinessSocketPair {
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

[[nodiscard]] inline bool write_native_readiness_byte(int fd, char value) noexcept {
    return ::write(fd, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value));
}

} // namespace io_native_readiness_example
