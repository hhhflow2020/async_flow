#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_send_to_some(TaskT &task, typename TaskT::Thread thread, int fd,
                                       const void *data, std::size_t size, const sockaddr *address,
                                       socklen_t address_size, IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    detail::clear_waiting(state);
    for (;;) {
        const ssize_t n =
            ::sendto(fd, data, size, detail::io_no_signal_flag(), address, address_size);
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
