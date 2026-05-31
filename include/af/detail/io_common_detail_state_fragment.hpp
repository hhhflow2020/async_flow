#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_detail_state_fragment.hpp is an io_common implementation fragment"
#endif

namespace detail {

template <typename TaskT>
[[nodiscard]] inline bool io_on_target_thread(typename TaskT::Thread thread) noexcept {
    return TaskT::Runtime::is_runtime_thread() &&
           TaskT::Runtime::current_thread_index() == TaskT::Runtime::thread_index(thread);
}

template <typename TaskT, typename NameFn>
[[nodiscard]] IoStatus io_socket_name(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size,
    NameFn name_fn) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (address == nullptr || address_size == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (!io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (name_fn(fd, address, address_size) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus arm_io_wait(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint32_t events,
    IoOpState& state) noexcept {
    const bool prefer_rearm =
        state.readiness_rearm_hint && state.readiness_fd == fd;
    state.wait = IoResult{fd, 0, 0};
    if (TaskT::Runtime::io_wait(thread, fd, events, &task, &state.wait, prefer_rearm)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Readiness;
        state.readiness_rearm_hint = true;
        state.readiness_fd = fd;
        return IoStatus::make_pending();
    }

    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    if (state.wait.error == EBADF || state.wait.error == ENOENT || state.wait.error == ENOSYS) {
        state.readiness_rearm_hint = false;
        state.readiness_fd = -1;
    }
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

[[nodiscard]] inline bool waiting_for_completion(const IoOpState& state) noexcept {
    return state.waiting && state.wait_kind == IoWaitKind::Completion;
}

inline void clear_waiting(IoOpState& state) noexcept {
    state.waiting = false;
    state.wait_kind = IoWaitKind::None;
    state.wait.completion_token = nullptr;
}

inline void clear_readiness_rearm_hint(IoOpState& state) noexcept {
    state.readiness_rearm_hint = false;
    state.readiness_fd = -1;
}

[[nodiscard]] inline bool cancelled_wait_ready(const IoOpState& state) noexcept {
    return state.waiting && state.wait.error == ECANCELED;
}

[[nodiscard]] inline bool io_wait_result_ready(const IoOpState& state) noexcept {
    return state.waiting &&
        (state.wait.events != 0U || state.wait.error != 0 || state.wait.result != 0);
}

[[nodiscard]] inline IoStatus consume_cancelled_wait(IoOpState& state) noexcept {
    clear_waiting(state);
    clear_readiness_rearm_hint(state);
    return IoStatus::failed(ECANCELED);
}

[[nodiscard]] inline bool uring_submit_error_can_fallback(int error) noexcept {
    return error == ENOSYS || error == EBUSY;
}

[[nodiscard]] inline bool uring_zero_copy_send_error_can_fallback(int error) noexcept {
    return error == ENOSYS || error == EINVAL
#ifdef EOPNOTSUPP
        || error == EOPNOTSUPP
#endif
#ifdef EAFNOSUPPORT
        || error == EAFNOSUPPORT
#endif
        ;
}

#if !defined(_WIN32)
[[nodiscard]] inline int io_max_iov() noexcept {
#if defined(IOV_MAX)
    return IOV_MAX;
#else
    return 1024;
#endif
}

[[nodiscard]] inline bool io_validate_iov(
    const iovec* iov,
    int iov_count,
    std::size_t& total_size,
    int& error) noexcept {
    total_size = 0;
    error = 0;
    if (iov_count < 0 || iov_count > io_max_iov()) {
        error = EINVAL;
        return false;
    }
    if (iov_count == 0) {
        return true;
    }
    if (iov == nullptr) {
        error = EINVAL;
        return false;
    }

    for (int i = 0; i < iov_count; ++i) {
        const std::size_t len = iov[i].iov_len;
        if (len != 0U && iov[i].iov_base == nullptr) {
            error = EINVAL;
            return false;
        }
        if (len > std::numeric_limits<std::size_t>::max() - total_size) {
            error = EOVERFLOW;
            return false;
        }
        total_size += len;
    }
    return true;
}
#endif

[[nodiscard]] inline IoStatus completed_uring_status(
    IoOpState& state,
    bool zero_is_closed = false) noexcept {
    clear_waiting(state);
    if (state.wait.error != 0) {
        return IoStatus::failed(state.wait.error);
    }
    if (state.wait.result < 0) {
        return IoStatus::failed(static_cast<int>(-state.wait.result));
    }
    if (zero_is_closed && state.wait.result == 0) {
        return IoStatus::make_closed();
    }
    return IoStatus::ready(static_cast<std::size_t>(state.wait.result));
}

inline void reset_multishot_completion_wait(IoOpState& state, int fd) noexcept {
    void* const completion_token = state.wait.completion_token;
    state.wait = IoResult{fd, 0, 0, 0};
    state.wait.completion_token = completion_token;
    state.waiting = true;
    state.wait_kind = IoWaitKind::Completion;
}

} // namespace detail
