#pragma once

#include "af/async_flow.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace io_native_readiness_example {

#if !defined(_WIN32)
inline bool set_native_fd_nonblocking_cloexec(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return false;
    }
    const int fd_flags = ::fcntl(fd, F_GETFD, 0);
    return fd_flags >= 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
}
#endif

struct NativeReadinessSocketPair {
    [[nodiscard]] bool create() noexcept {
#if !defined(_WIN32)
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        if (set_native_fd_nonblocking_cloexec(reader.get()) &&
            set_native_fd_nonblocking_cloexec(writer.get())) {
            return true;
        }
        reader.reset();
        writer.reset();
#endif
        return false;
    }

    af::UniqueFd reader{};
    af::UniqueFd writer{};
};

[[nodiscard]] inline bool write_native_readiness_byte(int fd, char value) noexcept {
#if !defined(_WIN32)
    return ::write(fd, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value));
#else
    static_cast<void>(fd);
    static_cast<void>(value);
    return false;
#endif
}

} // namespace io_native_readiness_example
