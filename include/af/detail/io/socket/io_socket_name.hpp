#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_getsockname(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size) noexcept {
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getsockname);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_getpeername(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size) noexcept {
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getpeername);
}
