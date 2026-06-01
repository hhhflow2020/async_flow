#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_statx(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                const char *path, int flags, std::uint32_t mask,
                                struct statx *output, IoOpState &state) noexcept {
    if (path == nullptr || output == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_statx(thread, dir_fd, path, flags, mask, output, &task,
                                        &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_fallocate(TaskT &task, typename TaskT::Thread thread, int fd, int mode,
                                    std::uint64_t offset, std::uint64_t length,
                                    IoOpState &state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (length == 0U) {
        return IoStatus::ready(0);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_fallocate(thread, fd, mode, offset, length, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
