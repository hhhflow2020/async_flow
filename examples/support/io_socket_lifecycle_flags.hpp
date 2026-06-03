#pragma once

#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>

namespace io_socket_lifecycle_example {

[[nodiscard]] inline int lifecycle_stream_socket_type() noexcept {
    int type = SOCK_STREAM;
#if defined(SOCK_NONBLOCK)
    type |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    type |= SOCK_CLOEXEC;
#endif
    return type;
}

[[nodiscard]] inline bool apply_lifecycle_socket_flags(int fd, int &error) noexcept {
    error = 0;
#if !defined(SOCK_NONBLOCK)
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        error = errno == 0 ? EIO : errno;
        return false;
    }
#endif

#if !defined(SOCK_CLOEXEC)
    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        error = errno == 0 ? EIO : errno;
        return false;
    }
#endif
    return true;
}

} // namespace io_socket_lifecycle_example
