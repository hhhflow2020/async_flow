#pragma once

#include "af/io_common.hpp"

namespace af {

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

template <typename TaskT>
[[nodiscard]] IoStatus io_setsockopt(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int level,
    int option,
    const void* value,
    socklen_t value_size) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (value == nullptr && value_size != 0U) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    static_cast<void>(level);
    static_cast<void>(option);
    static_cast<void>(value);
    static_cast<void>(value_size);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::setsockopt(fd, level, option, value, value_size) == 0) {
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

template <typename TaskT>
[[nodiscard]] IoStatus io_getsockopt(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int level,
    int option,
    void* value,
    socklen_t* value_size) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (value == nullptr || value_size == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    static_cast<void>(level);
    static_cast<void>(option);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::getsockopt(fd, level, option, value, value_size) == 0) {
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

template <typename TaskT>
[[nodiscard]] IoStatus io_getsockname(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size) noexcept {
#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    return IoStatus::failed(ENOSYS);
#else
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getsockname);
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_getpeername(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size) noexcept {
#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    return IoStatus::failed(ENOSYS);
#else
    return detail::io_socket_name(task, thread, fd, address, address_size, ::getpeername);
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_bind(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const sockaddr* address,
    socklen_t address_size) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (address == nullptr || address_size == 0U) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::bind(fd, address, address_size) == 0) {
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

template <typename TaskT>
[[nodiscard]] IoStatus io_listen(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int backlog) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }

#if defined(_WIN32)
    static_cast<void>(thread);
    static_cast<void>(backlog);
    return IoStatus::failed(ENOSYS);
#else
    if (!detail::io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (::listen(fd, backlog) == 0) {
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

template <typename TaskT>
[[nodiscard]] IoStatus io_accept_some(
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
    if (accepted_fd == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if ((address == nullptr) != (address_size == nullptr)) {
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
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        if (!completion.ready()) {
            return completion;
        }
        if (completion.bytes > static_cast<std::size_t>(INT_MAX)) {
            return IoStatus::failed(EOVERFLOW);
        }
        *accepted_fd = static_cast<int>(completion.bytes);
        return IoStatus::ready(0);
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_accept(
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
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }
    for (;;) {
#if defined(__linux__)
        const int accepted = ::accept4(fd, address, address_size, flags);
#else
        const int accepted = ::accept(fd, address, address_size);
#endif
        if (accepted >= 0) {
#if !defined(__linux__)
            int flag_error = 0;
            if (!detail::io_apply_accepted_flags(accepted, flags, flag_error)) {
                ::close(accepted);
                return IoStatus::failed(flag_error);
            }
#endif
            *accepted_fd = accepted;
            return IoStatus::ready(0);
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_accept_direct(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    sockaddr* address,
    socklen_t* address_size,
    int flags,
    int file_index,
    IoFixedFile<typename TaskT::Thread>* accepted_file,
    IoOpState& state) noexcept {
    if (accepted_file == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (fd < 0 || file_index < 0) {
        return IoStatus::failed(EBADF);
    }
    if ((address == nullptr) != (address_size == nullptr)) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.ready()) {
            return completion;
        }
        accepted_file->reset(thread, file_index);
        return IoStatus::ready(0);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_accept_direct(
            thread,
            fd,
            address,
            address_size,
            flags,
            file_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
#endif
}

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

template <typename TaskT>
[[nodiscard]] IoStatus io_connect(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const sockaddr* address,
    socklen_t address_size,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (address == nullptr || address_size == 0U) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_connect_in_progress(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return completion.ready() ? IoStatus::ready(0) : completion;
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
    if (TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_connect(
                thread,
                fd,
                address,
                address_size,
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
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_recv_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    void* data,
    std::size_t size,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state, true);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return completion;
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_recv(thread, fd, data, size, 0, &task, &state.wait)) {
            state.waiting = true;
            state.wait_kind = IoWaitKind::Completion;
            return IoStatus::make_pending();
        }
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }
    for (;;) {
        const ssize_t n = ::recv(fd, data, size, 0);
        if (n > 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }
        if (n == 0) {
            return IoStatus::make_closed();
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_recv_fixed_file_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    void* data,
    std::size_t size,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state, true);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_recv_fixed_file(
            thread,
            file_index,
            data,
            size,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus io_recv_multishot(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint16_t buffer_group,
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
        if (state.wait.result == 0) {
            if (!more) {
                detail::clear_waiting(state);
            }
            return IoStatus::make_closed();
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
    if (TaskT::Runtime::io_submit_recv_multishot(
            thread,
            fd,
            buffer_group,
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
[[nodiscard]] IoStatus io_recv_multishot(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint16_t buffer_group,
    std::uint16_t* buffer_id,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(buffer_group);
    static_cast<void>(buffer_id);
    static_cast<void>(state);
    static_cast<void>(flags);
    return IoStatus::failed(ENOSYS);
}
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

template <typename TaskT>
[[nodiscard]] IoStatus io_send_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if defined(_WIN32)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return completion;
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_send(
                thread,
                fd,
                data,
                size,
                static_cast<std::uint32_t>(detail::io_no_signal_flag()),
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
    for (;;) {
        const ssize_t n = ::send(fd, data, size, detail::io_no_signal_flag());
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

template <typename TaskT>
[[nodiscard]] IoStatus io_send_fixed_file_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const void* data,
    std::size_t size,
    IoOpState& state,
    std::uint32_t flags = static_cast<std::uint32_t>(detail::io_no_signal_flag())) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_send_fixed_file(
            thread,
            file_index,
            data,
            size,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_send_zc_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    bool skip_uring = false;
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.failed()) {
            return completion;
        }
        if (detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        if (!detail::uring_zero_copy_send_error_can_fallback(completion.error)) {
            return completion;
        }
        skip_uring = true;
    }

    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!skip_uring &&
        !resumed_from_readiness &&
        TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_send_zc(
                thread,
                fd,
                data,
                size,
                static_cast<std::uint32_t>(detail::io_no_signal_flag()),
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

    for (;;) {
        const ssize_t n = ::send(fd, data, size, detail::io_no_signal_flag());
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_zc_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    bool skip_uring = false;
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.failed()) {
            return completion;
        }
        if (detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        if (!detail::uring_zero_copy_send_error_can_fallback(completion.error)) {
            return completion;
        }
        skip_uring = true;
    }

    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!skip_uring &&
        !resumed_from_readiness &&
        TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_sendmsg_zc_iov(
                thread,
                fd,
                iov,
                iov_count,
                nullptr,
                0,
                static_cast<std::uint32_t>(detail::io_no_signal_flag()),
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

    msghdr message{};
    message.msg_iov = const_cast<iovec*>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::sendmsg(fd, &message, detail::io_no_signal_flag());
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
#endif
}
#endif

#if defined(__linux__)
template <typename TaskT>
[[nodiscard]] IoStatus io_sendfile_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int out_fd,
    int in_fd,
    IoOffset* offset,
    std::size_t count,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    if (out_fd < 0 || in_fd < 0) {
        return IoStatus::failed(EBADF);
    }

    detail::clear_waiting(state);
    for (;;) {
        const ssize_t n = ::sendfile(out_fd, in_fd, offset, count);
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, out_fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus io_shutdown(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int how,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (how != SHUT_RD && how != SHUT_WR && how != SHUT_RDWR) {
        return IoStatus::failed(EINVAL);
    }

    if (TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_shutdown(thread, fd, how, &task, &state.wait)) {
            state.waiting = true;
            state.wait_kind = IoWaitKind::Completion;
            return IoStatus::make_pending();
        }
        if (!detail::uring_submit_error_can_fallback(state.wait.error)) {
            return IoStatus::failed(state.wait.error);
        }
    }

    for (;;) {
        if (::shutdown(fd, how) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus io_splice_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int in_fd,
    IoOffset* off_in,
    int out_fd,
    IoOffset* off_out,
    std::size_t count,
    unsigned int flags,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    if (in_fd < 0 || out_fd < 0) {
        return IoStatus::failed(EBADF);
    }

    if (detail::waiting_for_completion(state)) {
        IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_splice_wait(task, thread, in_fd, out_fd, state);
        }
        if (completion.ready() && completion.bytes != 0U) {
            if (off_in != nullptr) {
                *off_in += static_cast<IoOffset>(completion.bytes);
            }
            if (off_out != nullptr) {
                *off_out += static_cast<IoOffset>(completion.bytes);
            }
        }
        return completion;
    }

    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        const std::int64_t input_offset = off_in == nullptr ? -1 : static_cast<std::int64_t>(*off_in);
        const std::int64_t output_offset = off_out == nullptr ? -1 : static_cast<std::int64_t>(*off_out);
        state.wait = IoResult{out_fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_splice(
                thread,
                in_fd,
                input_offset,
                out_fd,
                output_offset,
                count,
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

    for (;;) {
        const ssize_t n = ::splice(in_fd, off_in, out_fd, off_out, count, flags);
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_splice_wait(task, thread, in_fd, out_fd, state);
        }
        return IoStatus::failed(error);
    }
}
#else
template <typename TaskT>
[[nodiscard]] IoStatus io_sendfile_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int out_fd,
    int in_fd,
    IoOffset* offset,
    std::size_t count,
    IoOpState& state) noexcept {
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(out_fd);
    static_cast<void>(in_fd);
    static_cast<void>(offset);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_shutdown(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int how,
    IoOpState& state) noexcept {
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(how);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_splice_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int in_fd,
    IoOffset* off_in,
    int out_fd,
    IoOffset* off_out,
    std::size_t count,
    unsigned int flags,
    IoOpState& state) noexcept {
    if (count == 0U) {
        return IoStatus::ready(0);
    }
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(in_fd);
    static_cast<void>(off_in);
    static_cast<void>(out_fd);
    static_cast<void>(off_out);
    static_cast<void>(flags);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
}
#endif

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_recvv_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state, true);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return completion;
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_recvmsg_iov(
                thread,
                fd,
                iov,
                iov_count,
                nullptr,
                nullptr,
                0,
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

    msghdr message{};
    message.msg_iov = const_cast<iovec*>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::recvmsg(fd, &message, 0);
        if (n > 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }
        if (n == 0) {
            return IoStatus::make_closed();
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return IoStatus::failed(error);
    }
}

template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    std::size_t total_size = 0;
    int validation_error = 0;
    if (!detail::io_validate_iov(iov, iov_count, total_size, validation_error)) {
        return IoStatus::failed(validation_error);
    }
    if (total_size == 0U) {
        return IoStatus::ready(0);
    }

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return completion;
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_sendmsg_iov(
                thread,
                fd,
                iov,
                iov_count,
                nullptr,
                0,
                detail::io_no_signal_flag(),
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

    msghdr message{};
    message.msg_iov = const_cast<iovec*>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::sendmsg(fd, &message, detail::io_no_signal_flag());
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
        }

        const int error = errno;
        if (error == EINTR) {
            continue;
        }
        if (detail::io_would_block(error)) {
            return detail::arm_io_wait(task, thread, fd, io_writable, state);
        }
        return IoStatus::failed(error);
    }
}
#endif

} // namespace af
