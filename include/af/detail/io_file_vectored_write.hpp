#pragma once

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_writev_at(TaskT &task, typename TaskT::Thread thread, int fd,
                                    const iovec *iov, int iov_count, std::uint64_t offset,
                                    IoOpState &state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    std::size_t total_size = 0;
    int error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, error)) {
        return IoStatus::failed(error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_writev_at(thread, fd, iov, iov_count, offset, &task,
                                            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif
