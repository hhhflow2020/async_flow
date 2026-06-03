#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_shutdown(TaskT &task, typename TaskT::Thread thread, int fd, int how,
                                   IoOpState &state) noexcept {
    detail::clear_waiting(state);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (how != SHUT_RD && how != SHUT_WR && how != SHUT_RDWR) {
        return IoStatus::failed(EINVAL);
    }

    static_cast<void>(task);
    static_cast<void>(thread);

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
