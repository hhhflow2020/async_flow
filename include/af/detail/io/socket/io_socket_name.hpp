#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_getsockname(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size) noexcept {
#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    return IoStatus::failed(ENOSYS);
#else
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getsockname);
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_getpeername(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size) noexcept {
#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    return IoStatus::failed(ENOSYS);
#else
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getpeername);
#endif
}
