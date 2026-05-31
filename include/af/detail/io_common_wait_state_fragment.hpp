#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_wait_state_fragment.hpp is an io_common implementation fragment"
#endif

[[nodiscard]] inline bool waiting_for_completion(const IoOpState& state) noexcept {
    return state.waiting && state.wait_kind == IoWaitKind::Completion;
}

inline void clear_waiting(IoOpState& state) noexcept {
    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    state.wait.completion_token = nullptr;
}

[[nodiscard]] inline bool cancelled_wait_ready(const IoOpState& state) noexcept {
    return state.waiting && state.wait.error == ECANCELED;
}

[[nodiscard]] inline bool io_wait_result_ready(const IoOpState& state) noexcept {
    return state.waiting &&
        (state.wait.events != 0U || state.wait.error != 0 || state.wait.result != 0);
}

[[nodiscard]] inline IoStatus consume_cancelled_wait(IoOpState& state) noexcept {
    clear_waiting(state);
    return IoStatus::failed(ECANCELED);
}

inline void reset_multishot_completion_wait(IoOpState& state, int fd) noexcept {
    void* const completion_token = state.wait.completion_token;
    state.wait = IoResult{fd, 0, 0, 0};
    state.wait.completion_token = completion_token;
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
}
