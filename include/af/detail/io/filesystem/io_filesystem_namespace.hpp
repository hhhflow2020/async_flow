#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_mkdirat(TaskT &task, typename TaskT::Thread thread, int dir_fd,
                                  const char *path, std::uint32_t mode, IoOpState &state) noexcept {
    if (path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        if (::mkdirat(dir_fd, path, static_cast<mode_t>(mode)) == 0) {
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
[[nodiscard]] IoStatus io_symlinkat(TaskT &task, typename TaskT::Thread thread, const char *target,
                                    int new_dir_fd, const char *link_path,
                                    IoOpState &state) noexcept {
    if (target == nullptr || link_path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        if (::symlinkat(target, new_dir_fd, link_path) == 0) {
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
[[nodiscard]] IoStatus io_linkat(TaskT &task, typename TaskT::Thread thread, int old_dir_fd,
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
        if (::linkat(old_dir_fd, old_path, new_dir_fd, new_path, flags) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
