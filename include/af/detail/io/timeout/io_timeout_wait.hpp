#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_timeout(TaskT &task, typename TaskT::Thread thread,
                                       std::chrono::nanoseconds timeout,
                                       IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (timeout.count() <= 0) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_timer(state)) {
        if (!detail::io_wait_result_ready(state)) {
            return IoStatus::make_pending();
        }
        return detail::completed_timeout_status(state);
    }

    detail::clear_waiting(state);
    if (!TaskT::Runtime::io_backend_available(thread)) {
        return IoStatus::failed(ENOSYS);
    }

    state.wait = IoResult{-1, 0, 0, 0};
    if (TaskT::Runtime::io_timer_wait(thread, timeout, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Timer;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
