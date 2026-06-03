#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_socket(TaskT &task, typename TaskT::Thread thread, int domain, int type,
                                 int protocol, int *opened_fd, IoOpState &state,
                                 std::uint32_t flags = 0) noexcept {
    if (opened_fd == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

    detail::clear_waiting(state);

    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        const int fd = ::socket(domain, type, protocol);
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
