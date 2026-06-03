#pragma once

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "io_timeout_runtime.hpp"

namespace io_timeout_example {

[[nodiscard]] inline bool apply_timeout_socket_flags(int fd) noexcept {
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

struct TimeoutSocketPair {
    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        if (!apply_timeout_socket_flags(reader.get()) ||
            !apply_timeout_socket_flags(writer.get())) {
            return false;
        }
        return true;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

} // namespace io_timeout_example
