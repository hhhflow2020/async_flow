#if !defined(AF_IO_SOCKET_FRAGMENT_INCLUDE)
#error "io_socket_create_fragment.hpp is an io_socket implementation fragment"
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_socket(
    TaskT& task,
    typename TaskT::Thread thread,
    int domain,
    int type,
    int protocol,
    int* opened_fd,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    if (opened_fd == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(domain);
    static_cast<void>(type);
    static_cast<void>(protocol);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.ready()) {
            return completion;
        }
        if (completion.bytes > static_cast<std::size_t>(INT_MAX)) {
            return IoStatus::failed(EOVERFLOW);
        }
        *opened_fd = static_cast<int>(completion.bytes);
        return IoStatus::ready(0);
    }
    detail::clear_waiting(state);

    if (TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{-1, 0, 0, 0};
        if (TaskT::Runtime::io_submit_socket(
                thread,
                domain,
                type,
                protocol,
                flags,
                &task,
                &state.wait)) {
            state.waiting = true;
            state.wait_kind = IoWaitKind::Completion;
            return IoStatus::make_pending();
        }
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }

    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        const int fd = ::socket(domain, type, protocol);
        if (fd >= 0) {
            *opened_fd = fd;
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
#endif
}
