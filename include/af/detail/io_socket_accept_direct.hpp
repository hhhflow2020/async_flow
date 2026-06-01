#if !defined(AF_IO_SOCKET_DETAIL_INCLUDE)
#error "io_socket_accept_direct.hpp is internal to af/io_socket.hpp"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus
io_accept_direct(TaskT &task, typename TaskT::Thread thread, int fd,
                 sockaddr *address, socklen_t *address_size, int flags,
                 int file_index,
                 IoFixedFile<typename TaskT::Thread> *accepted_file,
                 IoOpState &state) noexcept {
  if (accepted_file == nullptr) {
    return IoStatus::failed(EINVAL);
  }
  if (fd < 0 || file_index < 0) {
    return IoStatus::failed(EBADF);
  }
  if ((address == nullptr) != (address_size == nullptr)) {
    return IoStatus::failed(EINVAL);
  }

#if defined(_WIN32)
  static_cast<void>(task);
  static_cast<void>(thread);
  static_cast<void>(state);
  static_cast<void>(flags);
  return IoStatus::failed(ENOSYS);
#else
  if (detail::waiting_for_completion(state)) {
    const IoStatus completion = detail::completed_uring_status(state);
    if (!completion.ready()) {
      return completion;
    }
    accepted_file->reset(thread, file_index);
    return IoStatus::ready(0);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_accept_direct(thread, fd, address, address_size,
                                              flags, file_index, &task,
                                              &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
#endif
}
