#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_openat2(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                  const char *path, const struct open_how *how, int *opened_fd,
                                  IoOpState &state) noexcept {
    if (opened_fd == nullptr || path == nullptr || how == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

#if defined(__linux__)
    for (;;) {
        const long fd = ::syscall(SYS_openat2, dir_fd, path, how, sizeof(*how));
        if (fd >= 0) {
            if (fd > INT_MAX) {
                ::close(static_cast<int>(fd));
                return IoStatus::failed(EOVERFLOW);
            }
            *opened_fd = static_cast<int>(fd);
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
#else
    static_cast<void>(dir_fd);
    return IoStatus::failed(ENOSYS);
#endif
}
