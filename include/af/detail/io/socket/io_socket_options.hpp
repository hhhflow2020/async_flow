#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_setsockopt(TaskT &task, typename TaskT::Thread thread, int fd, int level,
                                     int option, const void *value, socklen_t value_size) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (value == nullptr && value_size != 0U) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    static_cast<void>(level);
    static_cast<void>(option);
    static_cast<void>(value);
    static_cast<void>(value_size);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::setsockopt(fd, level, option, value, value_size) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_getsockopt(TaskT &task, typename TaskT::Thread thread, int fd, int level,
                                     int option, void *value, socklen_t *value_size) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (value == nullptr || value_size == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    static_cast<void>(level);
    static_cast<void>(option);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::getsockopt(fd, level, option, value, value_size) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
#endif
}
