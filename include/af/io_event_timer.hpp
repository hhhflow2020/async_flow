#pragma once

#include "af/io_datagram.hpp"

namespace af {

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_eventfd(TaskT &task, typename TaskT::Thread thread, int fd,
                                       std::uint64_t *value, IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (value == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    return detail::io_wait_uint64_counter_fd(task, thread, fd, value, state);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_timerfd(TaskT &task, typename TaskT::Thread thread, int fd,
                                       std::uint64_t *expirations, IoOpState &state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (expirations == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    return detail::io_wait_uint64_counter_fd(task, thread, fd, expirations, state);
}

} // namespace af
