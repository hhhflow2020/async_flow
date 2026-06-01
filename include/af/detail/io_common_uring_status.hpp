#if !defined(AF_IO_COMMON_DETAIL_INCLUDE)
#error "io_common_uring_status.hpp is internal to af/io_common.hpp"
#endif

[[nodiscard]] inline bool uring_submit_error_can_fallback(int error) noexcept {
  return error == ENOSYS || error == EBUSY;
}

[[nodiscard]] inline bool
uring_zero_copy_send_error_can_fallback(int error) noexcept {
  return error == ENOSYS || error == EINVAL
#ifdef EOPNOTSUPP
         || error == EOPNOTSUPP
#endif
#ifdef EAFNOSUPPORT
         || error == EAFNOSUPPORT
#endif
      ;
}

[[nodiscard]] inline IoStatus
completed_uring_status(IoOpState &state, bool zero_is_closed = false) noexcept {
  if (state.wait.completion_token != nullptr || !io_wait_result_ready(state)) {
    return IoStatus::make_pending();
  }
  clear_waiting(state);
  if (state.wait.error != 0) {
    return IoStatus::failed(state.wait.error);
  }
  if (state.wait.result < 0) {
    return IoStatus::failed(static_cast<int>(-state.wait.result));
  }
  if (zero_is_closed && state.wait.result == 0) {
    return IoStatus::make_closed();
  }
  return IoStatus::ready(static_cast<std::size_t>(state.wait.result));
}
