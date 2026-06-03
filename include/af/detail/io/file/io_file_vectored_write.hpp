#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_writev_at(TaskT &task, typename TaskT::Thread thread, int fd,
                                    const iovec *iov, int iov_count, std::uint64_t offset,
                                    IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    detail::clear_waiting(state);

    std::size_t total_size = 0;
    int error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, error)) {
        return IoStatus::failed(error);
    }
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    for (;;) {
        const ssize_t n = ::pwritev(fd, iov, iov_count, static_cast<off_t>(offset));
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
}
