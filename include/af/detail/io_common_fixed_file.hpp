#if !defined(AF_IO_COMMON_DETAIL_INCLUDE)
#error "io_common_fixed_file.hpp is internal to af/io_common.hpp"
#endif

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus
io_recvv_fixed_file_some(TaskT &task, typename TaskT::Thread thread,
                         int file_index, const iovec *iov, int iov_count,
                         IoOpState &state, std::uint32_t flags = 0) noexcept {
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
  if (file_index < 0) {
    return IoStatus::failed(EBADF);
  }

  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state, true);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_recvmsg_fixed_file_iov(
          thread, file_index, iov, iov_count, flags, &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_sendv_fixed_file_some(TaskT &task, typename TaskT::Thread thread,
                         int file_index, const iovec *iov, int iov_count,
                         IoOpState &state,
                         std::uint32_t flags = static_cast<std::uint32_t>(
                             detail::io_no_signal_flag())) noexcept {
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
  if (file_index < 0) {
    return IoStatus::failed(EBADF);
  }

  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_sendmsg_fixed_file_iov(
          thread, file_index, iov, iov_count, flags, &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif
