#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_openat(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                 const char *path, int flags, std::uint32_t mode, int *opened_fd,
                                 IoOpState &state) noexcept {
    if (opened_fd == nullptr || path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        const int fd = ::openat(dir_fd, path, flags, static_cast<mode_t>(mode));
        if (fd >= 0) {
            *opened_fd = fd;
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
