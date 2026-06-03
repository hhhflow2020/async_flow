#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_accept_some(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size, int *accepted_fd,
                                      IoOpState &state,
                                      int flags = detail::io_default_accept_flags()) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (accepted_fd == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if ((address == nullptr) != (address_size == nullptr)) {
        return IoStatus::failed(EINVAL);
    }
    *accepted_fd = -1;

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        if (!completion.ready()) {
            return completion;
        }
        if (completion.bytes > static_cast<std::size_t>(INT_MAX)) {
            return IoStatus::failed(EOVERFLOW);
        }
        *accepted_fd = static_cast<int>(completion.bytes);
        return IoStatus::ready(0);
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_accept(thread, fd, address, address_size, flags, &task,
                                             &state.wait)) {
            state.waiting = true;
            state.wait_kind = IoWaitKind::Completion;
            return IoStatus::make_pending();
        }
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }
    for (;;) {
#if defined(__linux__)
        const int accepted = ::accept4(fd, address, address_size, flags);
#else
        const int accepted = ::accept(fd, address, address_size);
#endif
        if (accepted >= 0) {
#if !defined(__linux__)
            int flag_error = 0;
            if (!detail::io_apply_accepted_flags(accepted, flags, flag_error)) {
                ::close(accepted);
                return IoStatus::failed(flag_error);
            }
#endif
            *accepted_fd = accepted;
            return IoStatus::ready(0);
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
}
