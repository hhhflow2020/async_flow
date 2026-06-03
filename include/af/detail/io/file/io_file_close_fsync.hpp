#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_fsync(TaskT &task, typename TaskT::Thread thread, int fd,
                                std::uint32_t flags, IoOpState &state) noexcept {
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    if (flags != 0U) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        if (::fsync(fd) == 0) {
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
[[nodiscard]] IoStatus io_close(TaskT &task, typename TaskT::Thread thread, UniqueFd &fd,
                                IoOpState &state) noexcept {
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }

    const int raw_fd = fd.get();
    if (raw_fd < 0) {
        return IoStatus::failed(EBADF);
    }

    static_cast<void>(fd.release());
    if (::close(raw_fd) == 0) {
        return IoStatus::ready(0);
    }
    return IoStatus::failed(errno == 0 ? EIO : errno);
}
