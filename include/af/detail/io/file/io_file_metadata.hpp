#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_statx(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                const char *path, int flags, std::uint32_t mask,
                                struct statx *output, IoOpState &state) noexcept {
    if (path == nullptr || output == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

#if defined(__linux__)
    for (;;) {
        if (::syscall(SYS_statx, dir_fd, path, flags, mask, output) == 0) {
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
    static_cast<void>(flags);
    static_cast<void>(mask);
    return IoStatus::failed(ENOSYS);
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_fallocate(TaskT &task, typename TaskT::Thread thread, int fd, int mode,
                                    std::uint64_t offset, std::uint64_t length,
                                    IoOpState &state) noexcept {
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (length == 0U) {
        return IoStatus::ready(0);
    }
    const auto max_offset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
    if (offset > max_offset || length > max_offset) {
        return IoStatus::failed(EOVERFLOW);
    }

#if defined(__linux__)
    for (;;) {
        if (::syscall(SYS_fallocate, fd, mode, static_cast<off_t>(offset),
                      static_cast<off_t>(length)) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
#else
    static_cast<void>(mode);
    static_cast<void>(offset);
    static_cast<void>(length);
    return IoStatus::failed(ENOSYS);
#endif
}
