#if !defined(AF_IO_FILESYSTEM_DETAIL_INCLUDE)
#error "io_filesystem_allocation.hpp is internal to af/io_filesystem.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_ftruncate(TaskT &task, typename TaskT::Thread thread,
                                    int fd, std::uint64_t length,
                                    IoOpState &state) noexcept {
  if (fd < 0) {
    return IoStatus::failed(EBADF);
  }
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_ftruncate(thread, fd, length, &task,
                                          &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
