#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_ftruncate(TaskT &task, typename TaskT::Thread thread, int fd,
                                    std::uint64_t length, IoOpState &state) noexcept {
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    detail::clear_waiting(state);
    static_cast<void>(task);
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    const auto max_length = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
    if (length > max_length) {
        return IoStatus::failed(EOVERFLOW);
    }

    for (;;) {
        if (::ftruncate(fd, static_cast<off_t>(length)) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
