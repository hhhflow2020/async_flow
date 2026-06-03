#pragma once

[[nodiscard]] inline bool waiting_for_timer(const IoOpState &state) noexcept {
    return state.waiting && state.wait_kind == IoWaitKind::Timer;
}

inline void clear_waiting(IoOpState &state) noexcept {
    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    state.wait.wait_token = nullptr;
}

[[nodiscard]] inline bool cancelled_wait_ready(const IoOpState &state) noexcept {
    return state.waiting && state.wait.error == ECANCELED;
}

[[nodiscard]] inline bool io_wait_result_ready(const IoOpState &state) noexcept {
    return state.waiting &&
           (state.wait.events != 0U || state.wait.error != 0 || state.wait.result != 0);
}

[[nodiscard]] inline IoStatus consume_cancelled_wait(IoOpState &state) noexcept {
    clear_waiting(state);
    return IoStatus::failed(ECANCELED);
}
