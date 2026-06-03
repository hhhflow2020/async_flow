#pragma once

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace asyncflow_examples {

[[nodiscard]] inline int socket_type_with_flags(int base) noexcept {
#if defined(SOCK_NONBLOCK)
    base |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    base |= SOCK_CLOEXEC;
#endif
    return base;
}

[[nodiscard]] inline bool apply_socket_flags(int fd) noexcept {
#if !defined(SOCK_NONBLOCK)
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 || ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return false;
    }
#endif

#if !defined(SOCK_CLOEXEC)
    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 || ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return false;
    }
#endif
    return true;
}

} // namespace asyncflow_examples
