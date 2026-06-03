#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_recv_from_some(TaskT &task, typename TaskT::Thread thread, int fd,
                                         void *data, std::size_t size, sockaddr *address,
                                         socklen_t *address_size, IoOpState &state) noexcept {
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
        const ssize_t n = ::recvfrom(fd, data, size, 0, address, address_size);
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
}
