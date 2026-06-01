#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_openat(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                 const char *path, int flags, std::uint32_t mode, int *opened_fd,
                                 IoOpState &state) noexcept {
    if (opened_fd == nullptr || path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.ready()) {
            return completion;
        }
        if (completion.bytes > static_cast<std::size_t>(INT_MAX)) {
            return IoStatus::failed(EOVERFLOW);
        }
        *opened_fd = static_cast<int>(completion.bytes);
        return IoStatus::ready(0);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_openat(thread, dir_fd, path, flags, mode, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus
io_openat_direct(TaskT &task, typename TaskT::Thread thread, int dir_fd, const char *path,
                 int flags, std::uint32_t mode, int file_index,
                 IoFixedFile<typename TaskT::Thread> *opened_file, IoOpState &state) noexcept {
    if (path == nullptr || opened_file == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.ready()) {
            return completion;
        }
        opened_file->reset(thread, file_index);
        return IoStatus::ready(0);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_openat_direct(thread, dir_fd, path, flags, mode, file_index,
                                                &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
