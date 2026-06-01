#if !defined(AF_IO_FILE_DETAIL_INCLUDE)
#error "io_file_read_positioned.hpp is internal to af/io_file.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus
io_read_at(TaskT &task, typename TaskT::Thread thread, int fd, void *data,
           std::size_t size, std::uint64_t offset, IoOpState &state) noexcept {
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);
  if (size == 0U) {
    return IoStatus::ready(0);
  }
  if (data == nullptr) {
    return IoStatus::failed(EINVAL);
  }

  state.wait = IoResult{fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_read_at(thread, fd, data, size, offset, &task,
                                        &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_readv_at(TaskT &task, typename TaskT::Thread thread,
                                   int fd, const iovec *iov, int iov_count,
                                   std::uint64_t offset,
                                   IoOpState &state) noexcept {
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  std::size_t total_size = 0;
  int error = 0;
  if (!detail::io_validate_iov(iov, iov_count, total_size, error)) {
    return IoStatus::failed(error);
  }
  if (total_size == 0U) {
    return IoStatus::ready(0);
  }

  state.wait = IoResult{fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_readv_at(thread, fd, iov, iov_count, offset,
                                         &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif
