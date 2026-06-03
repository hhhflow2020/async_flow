#pragma once

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus io_splice_some(TaskT &task, typename TaskT::Thread thread, int in_fd,
                                      IoOffset *off_in, int out_fd, IoOffset *off_out,
                                      std::size_t count, unsigned int flags,
                                      IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    if (in_fd < 0 || out_fd < 0) {
        return IoStatus::failed(EBADF);
    }

    detail::clear_waiting(state);

    for (;;) {
        const ssize_t n = ::splice(in_fd, off_in, out_fd, off_out, count, flags);
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_splice_wait(task, thread, in_fd, out_fd, state);
        }
        return IoStatus::failed(error);
    }
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus io_splice_some(TaskT &task, typename TaskT::Thread thread, int in_fd,
                                      IoOffset *off_in, int out_fd, IoOffset *off_out,
                                      std::size_t count, unsigned int flags,
                                      IoOpState &state) noexcept {
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(in_fd);
    static_cast<void>(off_in);
    static_cast<void>(out_fd);
    static_cast<void>(off_out);
    static_cast<void>(flags);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
}
#endif
