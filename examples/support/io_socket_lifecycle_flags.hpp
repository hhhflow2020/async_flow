#pragma once

#include "posix_socket_flags.hpp"

#include <sys/socket.h>

namespace io_socket_lifecycle_example {

[[nodiscard]] inline int lifecycle_stream_socket_type() noexcept {
    return asyncflow_examples::socket_type_with_flags(SOCK_STREAM);
}

[[nodiscard]] inline bool apply_lifecycle_socket_flags(int fd, int &error) noexcept {
    return asyncflow_examples::apply_socket_flags(fd, error);
}

} // namespace io_socket_lifecycle_example
