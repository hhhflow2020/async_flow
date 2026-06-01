#pragma once

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus
io_splice_some(TaskT &task, typename TaskT::Thread thread, int in_fd,
               IoOffset *off_in, int out_fd, IoOffset *off_out,
               std::size_t count, unsigned int flags,
               IoOpState &state) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  if (count == 0U) {
    return IoStatus::ready(0);
  }
  if (in_fd < 0 || out_fd < 0) {
    return IoStatus::failed(EBADF);
  }

  if (detail::waiting_for_completion(state)) {
    IoStatus completion = detail::completed_uring_status(state);
    if (completion.failed() && detail::io_would_block(completion.error)) {
      return detail::arm_splice_wait(task, thread, in_fd, out_fd, state);
    }
    if (completion.ready() && completion.bytes != 0U) {
      if (off_in != nullptr) {
        *off_in += static_cast<IoOffset>(completion.bytes);
      }
      if (off_out != nullptr) {
        *off_out += static_cast<IoOffset>(completion.bytes);
      }
    }
    return completion;
  }

  const bool resumed_from_readiness =
      state.waiting && state.wait_kind == IoWaitKind::Readiness;
  detail::clear_waiting(state);
  if (!resumed_from_readiness &&
      TaskT::Runtime::io_uring_backend_available(thread)) {
    const std::int64_t input_offset =
        off_in == nullptr ? -1 : static_cast<std::int64_t>(*off_in);
    const std::int64_t output_offset =
        off_out == nullptr ? -1 : static_cast<std::int64_t>(*off_out);
    state.wait = IoResult{out_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_splice(thread, in_fd, input_offset, out_fd,
                                         output_offset, count, flags, &task,
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
    const ssize_t n = ::splice(in_fd, off_in, out_fd, off_out, count, flags);
    if (n >= 0) {
      return IoStatus::ready(static_cast<std::size_t>(n));
    }

    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (detail::io_would_block(error)) {
      return detail::arm_splice_wait(task, thread, in_fd, out_fd, state);
    }
    return IoStatus::failed(error);
  }
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus
io_splice_some(TaskT &task, typename TaskT::Thread thread, int in_fd,
               IoOffset *off_in, int out_fd, IoOffset *off_out,
               std::size_t count, unsigned int flags,
               IoOpState &state) noexcept {
  if (count == 0U) {
    return IoStatus::ready(0);
  }
  static_cast<void>(task);
  static_cast<void>(thread);
  static_cast<void>(in_fd);
  static_cast<void>(off_in);
  static_cast<void>(out_fd);
  static_cast<void>(off_out);
  static_cast<void>(flags);
  static_cast<void>(state);
  return IoStatus::failed(ENOSYS);
}
#endif
