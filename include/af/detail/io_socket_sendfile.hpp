#pragma once

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus
io_sendfile_some(TaskT &task, typename TaskT::Thread thread, int out_fd,
                 int in_fd, IoOffset *offset, std::size_t count,
                 IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  if (count == 0U) {
    return IoStatus::ready(0);
  }
  if (out_fd < 0 || in_fd < 0) {
    return IoStatus::failed(EBADF);
  }

  detail::clear_waiting(state);
  for (;;) {
    const ssize_t n = ::sendfile(out_fd, in_fd, offset, count);
    if (n >= 0) {
      return IoStatus::ready(static_cast<std::size_t>(n));
    }

    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (detail::io_would_block(error)) {
      return detail::arm_io_wait(task, thread, out_fd, io_writable, state);
    }
    return IoStatus::failed(error);
  }
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus
io_sendfile_some(TaskT &task, typename TaskT::Thread thread, int out_fd,
                 int in_fd, IoOffset *offset, std::size_t count,
                 IoOpState &state) noexcept {
  if (count == 0U) {
    return IoStatus::ready(0);
  }
  static_cast<void>(task);
  static_cast<void>(thread);
  static_cast<void>(out_fd);
  static_cast<void>(in_fd);
  static_cast<void>(offset);
  static_cast<void>(state);
  return IoStatus::failed(ENOSYS);
}
#endif
