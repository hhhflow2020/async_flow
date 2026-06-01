#if !defined(AF_IO_FILESYSTEM_DETAIL_INCLUDE)
#error "io_filesystem_namespace.hpp is internal to af/io_filesystem.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus
io_mkdirat(TaskT &task, typename TaskT::Thread thread, int dir_fd,
           const char *path, std::uint32_t mode, IoOpState &state) noexcept {
  if (path == nullptr) {
    return IoStatus::failed(EINVAL);
  }
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{dir_fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_mkdirat(thread, dir_fd, path, mode, &task,
                                        &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_symlinkat(TaskT &task, typename TaskT::Thread thread, const char *target,
             int new_dir_fd, const char *link_path, IoOpState &state) noexcept {
  if (target == nullptr || link_path == nullptr) {
    return IoStatus::failed(EINVAL);
  }
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{new_dir_fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_symlinkat(thread, target, new_dir_fd, link_path,
                                          &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_linkat(TaskT &task, typename TaskT::Thread thread, int old_dir_fd,
          const char *old_path, int new_dir_fd, const char *new_path,
          std::uint32_t flags, IoOpState &state) noexcept {
  if (old_path == nullptr || new_path == nullptr) {
    return IoStatus::failed(EINVAL);
  }
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{old_dir_fd, 0, 0, 0};
  if (TaskT::Runtime::io_submit_linkat(thread, old_dir_fd, old_path, new_dir_fd,
                                       new_path, flags, &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
