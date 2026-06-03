#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_write_at(TaskT &task, typename TaskT::Thread thread, int fd,
                                   const void *data, std::size_t size, std::uint64_t offset,
                                   IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    for (;;) {
        const ssize_t n = ::pwrite(fd, data, size, static_cast<off_t>(offset));
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
}
