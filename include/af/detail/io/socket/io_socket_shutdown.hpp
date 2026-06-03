#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_shutdown(TaskT &task, typename TaskT::Thread thread, int fd, int how,
                                   IoOpState &state) noexcept {
#if defined(__linux__)
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
#endif
    detail::clear_waiting(state);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (how != SHUT_RD && how != SHUT_WR && how != SHUT_RDWR) {
        return IoStatus::failed(EINVAL);
    }

#if defined(__linux__)
    if (TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_shutdown(thread, fd, how, &task, &state.wait)) {
            state.waiting = true;
            state.wait_kind = IoWaitKind::Completion;
            return IoStatus::make_pending();
        }
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }
#else
    static_cast<void>(task);
    static_cast<void>(thread);
#endif

    for (;;) {
        if (::shutdown(fd, how) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
