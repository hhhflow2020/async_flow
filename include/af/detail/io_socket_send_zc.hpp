#if !defined(AF_IO_SOCKET_DETAIL_INCLUDE)
#error "io_socket_send_zc.hpp is internal to af/io_socket.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus
io_send_zc_some(TaskT &task, typename TaskT::Thread thread, int fd,
                const void *data, std::size_t size, IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  if (size == 0U) {
    return IoStatus::ready(0);
  }
  if (data == nullptr) {
    return IoStatus::failed(EINVAL);
  }

#if !defined(__linux__)
  static_cast<void>(task);
  static_cast<void>(thread);
  static_cast<void>(fd);
  static_cast<void>(state);
  return IoStatus::failed(ENOSYS);
#else
  bool skip_uring = false;
  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state);
    if (!completion.failed()) {
      return completion;
    }
    if (detail::io_would_block(completion.error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    if (!detail::uring_zero_copy_send_error_can_fallback(completion.error)) {
      return completion;
    }
    skip_uring = true;
  }

  const bool resumed_from_readiness =
      state.waiting && state.wait_kind == IoWaitKind::Readiness;
  detail::clear_waiting(state);
  if (!skip_uring && !resumed_from_readiness &&
      TaskT::Runtime::io_uring_backend_available(thread)) {
    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_send_zc(
            thread, fd, data, size,
            static_cast<std::uint32_t>(detail::io_no_signal_flag()), &task,
            &state.wait)) {
      state.waiting = true;
      state.wait_kind = IoWaitKind::Completion;
      return IoStatus::make_pending();
    }
    if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
      return IoStatus::failed(state.wait.error);
    }
  }

  for (;;) {
    const ssize_t n = ::send(fd, data, size, detail::io_no_signal_flag());
    if (n >= 0) {
      return IoStatus::ready(static_cast<std::size_t>(n));
    }

    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (detail::io_would_block(error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    return IoStatus::failed(error);
  }
#endif
}
