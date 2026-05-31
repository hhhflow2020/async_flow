#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_wait_arm_fragment.hpp is an io_common implementation fragment"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus arm_io_wait(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint32_t events,
    IoOpState& state) noexcept {
    state.wait = IoResult{fd, 0, 0};
    if (TaskT::Runtime::io_wait(thread, fd, events, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Readiness;
        return IoStatus::make_pending();
    }

    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    return IoStatus::failed(state.wait.error == 0 ? EINVAL : state.wait.error);
}

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus arm_splice_wait(
    TaskT& task,
    typename TaskT::Thread thread,
    int in_fd,
    int out_fd,
    IoOpState& state) noexcept {
    const bool out_waitable = io_fd_can_wait(out_fd);
    const bool in_waitable = io_fd_can_wait(in_fd);
    if (out_waitable && !io_poll_ready(out_fd, POLLOUT)) {
        return arm_io_wait(task, thread, out_fd, io_writable, state);
    }
    if (in_waitable && !io_poll_ready(in_fd, POLLIN)) {
        return arm_io_wait(task, thread, in_fd, io_readable, state);
    }
    if (out_waitable) {
        return arm_io_wait(task, thread, out_fd, io_writable, state);
    }
    if (in_waitable) {
        return arm_io_wait(task, thread, in_fd, io_readable, state);
    }
    return IoStatus::failed(EAGAIN);
}
#endif
