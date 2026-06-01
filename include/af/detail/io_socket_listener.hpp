#if !defined(AF_IO_SOCKET_DETAIL_INCLUDE)
#error "io_socket_listener.hpp is internal to af/io_socket.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_bind(TaskT &task, typename TaskT::Thread thread,
                               int fd, const sockaddr *address,
                               socklen_t address_size) noexcept {
  static_cast<void>(task);
  if (fd < 0) {
    return IoStatus::failed(EBADF);
  }
  if (address == nullptr || address_size == 0U) {
    return IoStatus::failed(EINVAL);
  }

#if defined(_WIN32)
  static_cast<void>(thread);
  return IoStatus::failed(ENOSYS);
#else
  if (!detail::io_on_target_thread<TaskT>(thread)) {
    return IoStatus::failed(EINVAL);
  }
  for (;;) {
    if (::bind(fd, address, address_size) == 0) {
      return IoStatus::ready(0);
    }
    const int error = errno == 0 ? EIO : errno;
    if (error == EINTR) {
      continue;
    }
    return IoStatus::failed(error);
  }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_listen(TaskT &task, typename TaskT::Thread thread,
                                 int fd, int backlog) noexcept {
  static_cast<void>(task);
  if (fd < 0) {
    return IoStatus::failed(EBADF);
  }

#if defined(_WIN32)
  static_cast<void>(thread);
  static_cast<void>(backlog);
  return IoStatus::failed(ENOSYS);
#else
  if (!detail::io_on_target_thread<TaskT>(thread)) {
    return IoStatus::failed(EINVAL);
  }
  for (;;) {
    if (::listen(fd, backlog) == 0) {
      return IoStatus::ready(0);
    }
    const int error = errno == 0 ? EIO : errno;
    if (error == EINTR) {
      continue;
    }
    return IoStatus::failed(error);
  }
#endif
}
