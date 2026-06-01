#if !defined(AF_IO_SOCKET_DETAIL_INCLUDE)
#error "io_socket_send_fixed_file.hpp is internal to af/io_socket.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus
io_send_fixed_file_some(TaskT &task, typename TaskT::Thread thread,
                        int file_index, const void *data, std::size_t size,
                        IoOpState &state,
                        std::uint32_t flags = static_cast<std::uint32_t>(
                            detail::io_no_signal_flag())) noexcept {
  if (detail::cancelled_wait_ready(state)) [[unlikely]] {
    return detail::consume_cancelled_wait(state);
  }
  if (size == 0U) {
    return IoStatus::ready(0);
  }
  if (file_index < 0) {
    return IoStatus::failed(EBADF);
  }
  if (data == nullptr) {
    return IoStatus::failed(EINVAL);
  }

  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_send_fixed_file(thread, file_index, data, size,
                                                flags, &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
