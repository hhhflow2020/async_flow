#if !defined(AF_IO_FILE_DETAIL_INCLUDE)
#error "io_file_close_fsync.hpp is internal to af/io_file.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_fsync(TaskT &task, typename TaskT::Thread thread,
                                int fd, std::uint32_t flags,
                                IoOpState &state) noexcept {
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_fsync(thread, fd, flags, &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_close(TaskT &task, typename TaskT::Thread thread,
                                UniqueFd &fd, IoOpState &state) noexcept {
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  const int raw_fd = fd.get();
  if (raw_fd < 0) {
    return IoStatus::failed(EBADF);
  }

  state.wait = IoResult{raw_fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_close(thread, raw_fd, &task, &state.wait)) {
    static_cast<void>(fd.release());
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
