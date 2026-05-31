#if !defined(AF_IO_SOCKET_FRAGMENT_INCLUDE)
#error "io_socket_recvmsg_multishot_fragment.hpp is an io_socket implementation fragment"
#endif

#if defined(__linux__)
[[nodiscard]] inline bool io_parse_recvmsg_multishot_buffer(
    const void* buffer,
    std::size_t buffer_size,
    std::size_t received_size,
    socklen_t name_capacity,
    std::size_t control_capacity,
    IoRecvmsgMultishotView& view,
    int& error) noexcept {
    view = IoRecvmsgMultishotView{};
    error = 0;
    if (buffer == nullptr ||
        received_size > buffer_size ||
        received_size < sizeof(detail::IoUringRecvmsgOut)) {
        error = EINVAL;
        return false;
    }

    const std::size_t name_capacity_size = static_cast<std::size_t>(name_capacity);
    const std::size_t header_size = sizeof(detail::IoUringRecvmsgOut);
    if (name_capacity_size > received_size - header_size ||
        control_capacity > received_size - header_size - name_capacity_size) {
        error = EINVAL;
        return false;
    }

    const auto* out = static_cast<const detail::IoUringRecvmsgOut*>(buffer);
    const std::size_t name_offset = header_size;
    const std::size_t control_offset = name_offset + name_capacity_size;
    const std::size_t payload_offset = control_offset + control_capacity;
    const std::size_t payload_available = received_size - payload_offset;
    if (payload_offset > std::numeric_limits<std::uint32_t>::max()) {
        error = EINVAL;
        return false;
    }

    view.name_offset = static_cast<std::uint32_t>(name_offset);
    view.name_size = static_cast<std::uint32_t>(
        std::min<std::size_t>(out->namelen, name_capacity_size));
    view.control_offset = static_cast<std::uint32_t>(control_offset);
    view.control_size = static_cast<std::uint32_t>(
        std::min<std::size_t>(out->controllen, control_capacity));
    view.payload_offset = static_cast<std::uint32_t>(payload_offset);
    view.payload_size = static_cast<std::uint32_t>(
        std::min<std::size_t>(out->payloadlen, payload_available));
    view.flags = out->flags;
    return true;
}
#else
[[nodiscard]] inline bool io_parse_recvmsg_multishot_buffer(
    const void* buffer,
    std::size_t buffer_size,
    std::size_t received_size,
    socklen_t name_capacity,
    std::size_t control_capacity,
    IoRecvmsgMultishotView& view,
    int& error) noexcept {
    static_cast<void>(buffer);
    static_cast<void>(buffer_size);
    static_cast<void>(received_size);
    static_cast<void>(name_capacity);
    static_cast<void>(control_capacity);
    view = IoRecvmsgMultishotView{};
    error = ENOSYS;
    return false;
}
#endif

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus io_recvmsg_multishot(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint16_t buffer_group,
    socklen_t name_capacity,
    std::size_t control_capacity,
    std::uint16_t* buffer_id,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (buffer_id == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
#if defined(MSG_WAITALL)
    if ((flags & static_cast<std::uint32_t>(MSG_WAITALL)) != 0U) {
        return IoStatus::failed(EINVAL);
    }
#endif
    *buffer_id = 0;

    if (detail::waiting_for_completion(state)) {
        if (!detail::io_wait_result_ready(state)) {
            return IoStatus::make_pending();
        }

        const bool more = (state.wait.events & io_more) != 0U;
        const bool selected = state.wait.buffer_selected();
        const std::uint16_t selected_id = state.wait.buffer_id();
        if (state.wait.error != 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(state.wait.error);
        }
        if (state.wait.result < 0) {
            detail::clear_waiting(state);
            return IoStatus::failed(static_cast<int>(-state.wait.result));
        }
        if (!selected) {
            if (!more) {
                detail::clear_waiting(state);
            }
            return IoStatus::failed(ENOBUFS);
        }

        *buffer_id = selected_id;
        const auto bytes = static_cast<std::size_t>(state.wait.result);
        if (more) {
            detail::reset_multishot_completion_wait(state, fd);
        } else {
            detail::clear_waiting(state);
        }
        return IoStatus::ready(bytes);
    }

    detail::clear_waiting(state);
    if (!TaskT::Runtime::io_uring_backend_available(thread)) {
        return IoStatus::failed(ENOSYS);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_recvmsg_multishot(
            thread,
            fd,
            buffer_group,
            name_capacity,
            control_capacity,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus io_recvmsg_multishot(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint16_t buffer_group,
    socklen_t name_capacity,
    std::size_t control_capacity,
    std::uint16_t* buffer_id,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(buffer_group);
    static_cast<void>(name_capacity);
    static_cast<void>(control_capacity);
    static_cast<void>(buffer_id);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
}
#endif
