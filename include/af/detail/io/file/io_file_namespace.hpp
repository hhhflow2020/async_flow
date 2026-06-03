#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_renameat(TaskT &task, typename TaskT::Thread thread, int old_dir_fd,
                                   const char *old_path, int new_dir_fd, const char *new_path,
                                   std::uint32_t flags, IoOpState &state) noexcept {
    if (old_path == nullptr || new_path == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        int rc = -1;
        if (flags == 0U) {
            rc = ::renameat(old_dir_fd, old_path, new_dir_fd, new_path);
        } else {
#if defined(__linux__)
            rc = static_cast<int>(
                ::syscall(SYS_renameat2, old_dir_fd, old_path, new_dir_fd, new_path, flags));
#else
            return IoStatus::failed(ENOSYS);
#endif
        }
        if (rc == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus io_unlinkat(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                   const char *path, int flags, IoOpState &state) noexcept {
    if (path == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        if (::unlinkat(dir_fd, path, flags) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
