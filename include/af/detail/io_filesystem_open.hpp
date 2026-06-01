#if !defined(AF_IO_FILESYSTEM_DETAIL_INCLUDE)
#error "io_filesystem_open.hpp is internal to af/io_filesystem.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_openat2(TaskT &task, typename TaskT::Thread thread,
                                  int dir_fd, const char *path,
                                  const struct open_how *how, int *opened_fd,
                                  IoOpState &state) noexcept {
  if (opened_fd == nullptr || path == nullptr || how == nullptr) {
    return IoStatus::failed(EINVAL);
  }
  *opened_fd = -1;

  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state);
    if (!completion.ready()) {
      return completion;
    }
    if (completion.bytes > static_cast<std::size_t>(INT_MAX)) {
      return IoStatus::failed(EOVERFLOW);
    }
    *opened_fd = static_cast<int>(completion.bytes);
    return IoStatus::ready(0);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{dir_fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_openat2(thread, dir_fd, path, how, &task,
                                        &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
