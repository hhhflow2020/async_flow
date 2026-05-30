#pragma once

#include "af/io_event_timer.hpp"

namespace af {

namespace detail {

[[nodiscard]] inline bool io_timeout_expired_error(int error) noexcept {
#if defined(ETIME)
    if (error == ETIME) {
        return true;
    }
#endif
    return error == ETIMEDOUT;
}

[[nodiscard]] inline IoStatus completed_uring_timeout_status(IoOpState& state) noexcept {
    clear_waiting(state);
    if (io_timeout_expired_error(state.wait.error)) {
        return IoStatus::ready(0);
    }
    if (state.wait.error != 0) {
        return IoStatus::failed(state.wait.error);
    }
    if (state.wait.result < 0) {
        const int error = static_cast<int>(-state.wait.result);
        if (io_timeout_expired_error(error)) {
            return IoStatus::ready(0);
        }
        return IoStatus::failed(error);
    }
    return IoStatus::ready(static_cast<std::size_t>(state.wait.result));
}

} // namespace detail

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_timeout(
    TaskT& task,
    typename TaskT::Thread thread,
    std::chrono::nanoseconds timeout,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (timeout.count() <= 0) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        if (!detail::io_wait_result_ready(state)) {
            return IoStatus::make_pending();
        }
        return detail::completed_uring_timeout_status(state);
    }

    detail::clear_waiting(state);
    if (!TaskT::Runtime::io_uring_backend_available(thread)) {
        return IoStatus::failed(ENOSYS);
    }

    state.wait = IoResult{-1, 0, 0, 0};
    if (TaskT::Runtime::io_submit_timeout(thread, timeout, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus arm_io_timeout(
    TaskT& task,
    typename TaskT::Thread thread,
    IoDeadline& deadline,
    IoOpState& io_state) noexcept {
#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(deadline);
    static_cast<void>(io_state);
    return IoStatus::failed(ENOSYS);
#else
    if (!deadline.configured()) {
        return IoStatus::failed(EINVAL);
    }

    auto consume_deadline_wait = [&]() noexcept -> IoStatus {
        if (deadline.ring_timeout) {
            return io_wait_timeout(task, thread, deadline.delay, deadline.wait);
        }
        return io_wait_timerfd(
            task,
            thread,
            deadline.timer.get(),
            &deadline.expirations,
            deadline.wait);
    };

    if (deadline.timeout_cancel_pending) {
        if (deadline.wait.wait.completion_token != nullptr ||
            !detail::io_wait_result_ready(deadline.wait)) {
            return IoStatus::make_pending();
        }
        const IoStatus timeout = consume_deadline_wait();
        if (timeout.failed() && timeout.error != ECANCELED) {
            deadline.reset_runtime();
            return timeout;
        }
        deadline.reset_runtime();
        return IoStatus::ready(0);
    }

    if (deadline.cancel_pending) {
        if (io_state.wait.completion_token != nullptr ||
            !detail::io_wait_result_ready(io_state)) {
            return IoStatus::make_pending();
        }
        detail::clear_waiting(io_state);
        deadline.reset_runtime();
        return IoStatus::failed(ETIMEDOUT);
    }

    if (deadline.armed && detail::io_wait_result_ready(io_state)) {
        if (detail::io_wait_result_ready(deadline.wait)) {
            static_cast<void>(consume_deadline_wait());
        } else if (deadline.wait.waiting) {
            if (deadline.ring_timeout) {
                if (!TaskT::Runtime::cancel_io(thread, deadline.wait)) {
                    const int error = deadline.wait.wait.error == 0 ? EIO : deadline.wait.wait.error;
                    deadline.reset_runtime();
                    return IoStatus::failed(error);
                }
                deadline.timeout_cancel_pending = true;
                deadline.armed = false;
                return IoStatus::make_pending();
            }
            static_cast<void>(TaskT::Runtime::cancel_io(thread, deadline.wait));
        }
        if (!deadline.ring_timeout) {
            int error = 0;
            static_cast<void>(disarm_timerfd(deadline.timer.get(), error));
        }
        deadline.reset_runtime();
        return IoStatus::ready(0);
    }

    if (deadline.armed && detail::io_wait_result_ready(deadline.wait)) {
        const IoStatus timeout = consume_deadline_wait();
        if (!timeout.ready()) {
            deadline.reset_runtime();
            return timeout.failed() ? timeout : IoStatus::failed(EIO);
        }

        const IoWaitKind io_kind = io_state.wait_kind;
        if (!TaskT::Runtime::cancel_io(thread, io_state)) {
            const int error = io_state.wait.error == 0 ? EIO : io_state.wait.error;
            deadline.reset_runtime();
            return IoStatus::failed(error);
        }

        if (io_kind == IoWaitKind::Completion) {
            deadline.cancel_pending = true;
            deadline.armed = false;
            return IoStatus::make_pending();
        }

        detail::clear_waiting(io_state);
        deadline.reset_runtime();
        return IoStatus::failed(ETIMEDOUT);
    }

    if (deadline.armed) {
        return IoStatus::make_pending();
    }
    if (!io_state.waiting) {
        return IoStatus::failed(EINVAL);
    }

    deadline.wait.reset();
    deadline.expirations = 0;
    if (TaskT::Runtime::io_uring_backend_available(thread)) {
        const IoStatus status = io_wait_timeout(task, thread, deadline.delay, deadline.wait);
        if (status.pending()) {
            deadline.armed = true;
            deadline.ring_timeout = true;
            return status;
        }
        if (status.failed() && status.error != ENOSYS && status.error != EBUSY) {
            return status;
        }
        deadline.wait.reset();
    }

    if (!deadline.timer) {
        deadline.timer = make_timerfd();
        if (!deadline.timer) {
            return IoStatus::failed(errno == 0 ? EIO : errno);
        }
    }

    int error = 0;
    if (!arm_timerfd_after(deadline.timer.get(), deadline.delay, error)) {
        return IoStatus::failed(error);
    }

    const IoStatus status = io_wait_timerfd(
        task,
        thread,
        deadline.timer.get(),
        &deadline.expirations,
        deadline.wait);
    if (status.pending()) {
        deadline.armed = true;
        deadline.ring_timeout = false;
    }
    return status;
#endif
}

} // namespace af
