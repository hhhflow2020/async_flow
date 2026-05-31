#if !defined(AF_IO_SOCKET_FRAGMENT_INCLUDE)
#error "io_socket_accept_multishot_fragment.hpp is an io_socket implementation fragment"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_accept_multishot(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size,
    int* accepted_fd,
    IoOpState& state,
    int flags = detail::io_default_accept_flags()) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (accepted_fd == nullptr || fd < 0) {
        return IoStatus::failed(accepted_fd == nullptr ? EINVAL : EBADF);
    }
    if (address != nullptr || address_size != nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *accepted_fd = -1;

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        if (!detail::io_wait_result_ready(state)) {
            return IoStatus::make_pending();
        }
        const bool more = (state.wait.events & io_more) != 0U;
        if (state.wait.error != 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(state.wait.error);
        }
        if (state.wait.result < 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(static_cast<int>(-state.wait.result));
        }
        if (state.wait.result > static_cast<std::int64_t>(INT_MAX)) {
            if (!more) {
                detail::clear_waiting(state);
            }
            return IoStatus::failed(EOVERFLOW);
        }

        *accepted_fd = static_cast<int>(state.wait.result);
        if (more) {
            detail::reset_multishot_completion_wait(state, fd);
        } else {
            detail::clear_waiting(state);
        }
        return IoStatus::ready(0);
    }

    detail::clear_waiting(state);
    if (!TaskT::Runtime::io_uring_backend_available(thread)) {
        return IoStatus::failed(ENOSYS);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_accept_multishot(
            thread,
            fd,
            address,
            address_size,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
#endif
}
