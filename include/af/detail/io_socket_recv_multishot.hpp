#pragma once

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus io_recv_multishot(TaskT &task, typename TaskT::Thread thread, int fd,
                                         std::uint16_t buffer_group, std::uint16_t *buffer_id,
                                         IoOpState &state, std::uint32_t flags = 0) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (buffer_id == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    *buffer_id = 0;

    if (detail::waiting_for_completion(state)) {
        if (!detail::io_wait_result_ready(state)) {
            return IoStatus::make_pending();
        }

        const bool more = (state.wait.events & io_more) != 0U;
        const bool selected = state.wait.buffer_selected();
        const std::uint16_t selected_id = state.wait.buffer_id();
        if (state.wait.error != 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(state.wait.error);
        }
        if (state.wait.result < 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(static_cast<int>(-state.wait.result));
        }
        if (state.wait.result == 0) {
            if (!more) {
                detail::clear_waiting(state);
            }
            return IoStatus::make_closed();
        }
        if (!selected) {
            if (!more) {
                detail::clear_waiting(state);
            }
            return IoStatus::failed(ENOBUFS);
        }

        *buffer_id = selected_id;
        const auto bytes = static_cast<std::size_t>(state.wait.result);
        if (more) {
            detail::reset_multishot_completion_wait(state, fd);
        } else {
            detail::clear_waiting(state);
        }
        return IoStatus::ready(bytes);
    }

    detail::clear_waiting(state);
    if (!TaskT::Runtime::io_uring_backend_available(thread)) {
        return IoStatus::failed(ENOSYS);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_recv_multishot(thread, fd, buffer_group, flags, &task,
                                                 &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus io_recv_multishot(TaskT &task, typename TaskT::Thread thread, int fd,
                                         std::uint16_t buffer_group, std::uint16_t *buffer_id,
                                         IoOpState &state, std::uint32_t flags = 0) noexcept {
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(buffer_group);
    static_cast<void>(buffer_id);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
}
#endif
