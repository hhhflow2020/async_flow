#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_connect(TaskT &task, typename TaskT::Thread thread,
                                  int fd, const sockaddr *address,
                                  socklen_t address_size,
                                  IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  if (address == nullptr || address_size == 0U) {
    return IoStatus::failed(EINVAL);
  }

#if defined(_WIN32)
  static_cast<void>(task);
  static_cast<void>(thread);
  static_cast<void>(fd);
  static_cast<void>(address);
  static_cast<void>(address_size);
  static_cast<void>(state);
  return IoStatus::failed(ENOSYS);
#else
  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state);
    if (completion.failed() &&
        detail::io_connect_in_progress(completion.error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    return completion.ready() ? IoStatus::ready(0) : completion;
  }
  const bool resumed_from_readiness =
      state.waiting && state.wait_kind == IoWaitKind::Readiness;
  detail::clear_waiting(state);
  if (resumed_from_readiness) {
    const int error = detail::io_socket_connect_error(fd);
    if (error == 0 || error == EISCONN) {
      return IoStatus::ready(0);
    }
    if (detail::io_connect_in_progress(error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    return IoStatus::failed(error);
  }
  if (TaskT::Runtime::io_uring_backend_available(thread)) {
    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_connect(thread, fd, address, address_size,
                                          &task, &state.wait)) {
      state.waiting = true;
      state.wait_kind = IoWaitKind::Completion;
      return IoStatus::make_pending();
    }
    if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
      return IoStatus::failed(state.wait.error);
    }
  }
  for (;;) {
    if (::connect(fd, address, address_size) == 0) {
      return IoStatus::ready(0);
    }

    const int error = errno;
    if (error == EISCONN) {
      return IoStatus::ready(0);
    }
    if (detail::io_connect_in_progress(error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    return IoStatus::failed(error);
  }
#endif
}
