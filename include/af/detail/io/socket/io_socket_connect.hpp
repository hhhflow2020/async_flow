#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus io_connect(TaskT &task, typename TaskT::Thread thread, int fd,
                                  const sockaddr *address, socklen_t address_size,
                                  IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (address == nullptr || address_size == 0U) {
        return IoStatus::failed(EINVAL);
    }

    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (resumed_from_readiness) {
        const int error = detail::io_socket_connect_error(fd);
        if (error == 0 || error == EISCONN) {
            return IoStatus::ready(0);
        }
        if (detail::io_connect_in_progress(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
    for (;;) {
        if (::connect(fd, address, address_size) == 0) {
            return IoStatus::ready(0);
        }

        const int error = errno;
        if (error == EISCONN) {
            return IoStatus::ready(0);
        }
        if (detail::io_connect_in_progress(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
}
