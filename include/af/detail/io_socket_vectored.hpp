#pragma once

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_recvv_some(TaskT &task, typename TaskT::Thread thread,
                                     int fd, const iovec *iov, int iov_count,
                                     IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  std::size_t total_size = 0;
  int validation_error = 0;
  if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
    return IoStatus::failed(validation_error);
  }
  if (total_size == 0U) {
    return IoStatus::ready(0);
  }

  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state, true);
    if (completion.failed() && detail::io_would_block(completion.error)) {
      return detail::arm_io_wait(task, thread, fd, io_readable, state);
    }
    return completion;
  }
  const bool resumed_from_readiness =
      state.waiting && state.wait_kind == IoWaitKind::Readiness;
  detail::clear_waiting(state);
  if (!resumed_from_readiness &&
      TaskT::Runtime::io_uring_backend_available(thread)) {
    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_recvmsg_iov(thread, fd, iov, iov_count,
                                              nullptr, nullptr, 0, &task,
                                              &state.wait)) {
      state.waiting = true;
      state.wait_kind = IoWaitKind::Completion;
      return IoStatus::make_pending();
    }
    if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
      return IoStatus::failed(state.wait.error);
    }
  }

  msghdr message{};
  message.msg_iov = const_cast<iovec *>(iov);
  message.msg_iovlen = static_cast<std::size_t>(iov_count);
  for (;;) {
    const ssize_t n = ::recvmsg(fd, &message, 0);
    if (n > 0) {
      return IoStatus::ready(static_cast<std::size_t>(n));
    }
    if (n == 0) {
      return IoStatus::make_closed();
    }

    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (detail::io_would_block(error)) {
      return detail::arm_io_wait(task, thread, fd, io_readable, state);
    }
    return IoStatus::failed(error);
  }
}

template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_some(TaskT &task, typename TaskT::Thread thread,
                                     int fd, const iovec *iov, int iov_count,
                                     IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  std::size_t total_size = 0;
  int validation_error = 0;
  if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
    return IoStatus::failed(validation_error);
  }
  if (total_size == 0U) {
    return IoStatus::ready(0);
  }

  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state);
    if (completion.failed() && detail::io_would_block(completion.error)) {
      return detail::arm_io_wait(task, thread, fd, io_writable, state);
    }
    return completion;
  }
  const bool resumed_from_readiness =
      state.waiting && state.wait_kind == IoWaitKind::Readiness;
  detail::clear_waiting(state);
  if (!resumed_from_readiness &&
      TaskT::Runtime::io_uring_backend_available(thread)) {
    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_sendmsg_iov(
            thread, fd, iov, iov_count, nullptr, 0, detail::io_no_signal_flag(),
            &task, &state.wait)) {
      state.waiting = true;
      state.wait_kind = IoWaitKind::Completion;
      return IoStatus::make_pending();
    }
    if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
      return IoStatus::failed(state.wait.error);
    }
  }

  msghdr message{};
  message.msg_iov = const_cast<iovec *>(iov);
  message.msg_iovlen = static_cast<std::size_t>(iov_count);
  for (;;) {
    const ssize_t n = ::sendmsg(fd, &message, detail::io_no_signal_flag());
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
}
#endif
