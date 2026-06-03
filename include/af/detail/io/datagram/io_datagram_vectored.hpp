#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_recvv_from_some(TaskT &task, typename TaskT::Thread thread, int fd,
                                          const iovec *iov, int iov_count, sockaddr *address,
                                          socklen_t *address_size, IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    detail::clear_waiting(state);

    msghdr message{};
    message.msg_name = address;
    message.msg_namelen = address_size == nullptr ? 0 : *address_size;
    message.msg_iov = const_cast<iovec *>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::recvmsg(fd, &message, 0);
        if (n >= 0) {
            if (address_size != nullptr) {
                *address_size = message.msg_namelen;
            }
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

template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_to_some(TaskT &task, typename TaskT::Thread thread, int fd,
                                        const iovec *iov, int iov_count, const sockaddr *address,
                                        socklen_t address_size, IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    detail::clear_waiting(state);

    msghdr message{};
    message.msg_name = const_cast<sockaddr *>(address);
    message.msg_namelen = address_size;
    message.msg_iov = const_cast<iovec *>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::sendmsg(fd, &message, detail::io_no_signal_flag());
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
