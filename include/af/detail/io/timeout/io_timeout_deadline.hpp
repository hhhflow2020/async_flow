#pragma once

template <typename TaskT>
[[nodiscard]] IoStatus arm_io_timeout(TaskT &task, typename TaskT::Thread thread,
                                      IoDeadline &deadline, IoOpState &io_state) noexcept {
    if (!deadline.configured()) {
        return IoStatus::failed(EINVAL);
    }

    auto consume_deadline_wait = [&]() noexcept -> IoStatus {
        if (deadline.runtime_timer) {
            return io_wait_timeout(task, thread, deadline.delay, deadline.wait);
        }
        return io_wait_timerfd(task, thread, deadline.timer.get(), &deadline.expirations,
                               deadline.wait);
    };

    if (deadline.timeout_cancel_pending) {
        if (deadline.wait.wait.wait_token != nullptr ||
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
        if (io_state.wait.wait_token != nullptr || !detail::io_wait_result_ready(io_state)) {
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
            if (deadline.runtime_timer) {
                if (!TaskT::Runtime::cancel_io(thread, deadline.wait)) {
                    const int error =
                        deadline.wait.wait.error == 0 ? EIO : deadline.wait.wait.error;
                    deadline.reset_runtime();
                    return IoStatus::failed(error);
                }
                if (deadline.wait.wait.wait_token == nullptr &&
                    detail::io_wait_result_ready(deadline.wait)) {
                    const IoStatus timeout = consume_deadline_wait();
                    if (timeout.failed() && timeout.error != ECANCELED) {
                        deadline.reset_runtime();
                        return timeout;
                    }
                    deadline.reset_runtime();
                    return IoStatus::ready(0);
                }
                deadline.timeout_cancel_pending = true;
                deadline.armed = false;
                return IoStatus::make_pending();
            }
            static_cast<void>(TaskT::Runtime::cancel_io(thread, deadline.wait));
        }
#if defined(__linux__)
        if (!deadline.runtime_timer) {
            int error = 0;
            static_cast<void>(disarm_timerfd(deadline.timer.get(), error));
        }
#endif
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

        if (io_kind == IoWaitKind::Timer) {
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
    if (TaskT::Runtime::io_backend_available(thread)) {
        const IoStatus status = io_wait_timeout(task, thread, deadline.delay, deadline.wait);
        if (status.pending()) {
            deadline.armed = true;
            deadline.runtime_timer = true;
            return status;
        }
        if (status.failed() && status.error != ENOSYS && status.error != EBUSY) {
            return status;
        }
        deadline.wait.reset();
    }

#if !defined(__linux__)
    return IoStatus::failed(ENOSYS);
#else
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

    const IoStatus status =
        io_wait_timerfd(task, thread, deadline.timer.get(), &deadline.expirations, deadline.wait);
    if (status.pending()) {
        deadline.armed = true;
        deadline.runtime_timer = false;
    }
    return status;
#endif
}
