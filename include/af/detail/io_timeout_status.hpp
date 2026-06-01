#pragma once

namespace detail {

[[nodiscard]] inline bool io_timeout_expired_error(int error) noexcept {
#if defined(ETIME)
  if (error == ETIME) {
    return true;
  }
#endif
  return error == ETIMEDOUT;
}

[[nodiscard]] inline IoStatus
completed_uring_timeout_status(IoOpState &state) noexcept {
  if (state.wait.completion_token != nullptr || !io_wait_result_ready(state)) {
    return IoStatus::make_pending();
  }
  clear_waiting(state);
  if (io_timeout_expired_error(state.wait.error)) {
    return IoStatus::ready(0);
  }
  if (state.wait.error != 0) {
    return IoStatus::failed(state.wait.error);
  }
  if (state.wait.result < 0) {
    const int error = static_cast<int>(-state.wait.result);
    if (io_timeout_expired_error(error)) {
      return IoStatus::ready(0);
    }
    return IoStatus::failed(error);
  }
  return IoStatus::ready(static_cast<std::size_t>(state.wait.result));
}

} // namespace detail
