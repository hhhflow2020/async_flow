#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus
io_fsync_fixed_file(TaskT &task, typename TaskT::Thread thread, int file_index,
                    IoOpState &state, std::uint32_t flags = 0) noexcept {
  if (detail::waiting_for_completion(state)) {
    return detail::completed_uring_status(state);
  }
  detail::clear_waiting(state);

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_fsync_fixed_file(thread, file_index, flags,
                                                 &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus
io_read_fixed_file_at(TaskT &task, typename TaskT::Thread thread,
                      int file_index, void *data, std::size_t size,
                      std::uint64_t offset, std::uint16_t buffer_index,
                      IoOpState &state) noexcept {
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

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_read_fixed_file_at(thread, file_index, data,
                                                   size, offset, buffer_index,
                                                   &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_read_fixed_file_at(TaskT &task, typename TaskT::Thread thread,
                      int file_index, IoFixedBuffer buffer,
                      std::uint64_t offset, IoOpState &state) noexcept {
  return io_read_fixed_file_at(task, thread, file_index, buffer.data,
                               buffer.size, offset, buffer.index, state);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_write_fixed_file_at(TaskT &task, typename TaskT::Thread thread,
                       int file_index, const void *data, std::size_t size,
                       std::uint64_t offset, std::uint16_t buffer_index,
                       IoOpState &state) noexcept {
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

  state.wait = IoResult{file_index, 0, 0, 0};
  if (TaskT::Runtime::io_submit_write_fixed_file_at(thread, file_index, data,
                                                    size, offset, buffer_index,
                                                    &task, &state.wait)) {
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
    return IoStatus::make_pending();
  }
  return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_write_fixed_file_at(TaskT &task, typename TaskT::Thread thread,
                       int file_index, IoFixedBuffer buffer,
                       std::uint64_t offset, IoOpState &state) noexcept {
  return io_write_fixed_file_at(task, thread, file_index, buffer.data,
                                buffer.size, offset, buffer.index, state);
}
#endif
