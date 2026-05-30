#pragma once

#include "af/io_socket.hpp"

namespace af {

template <typename TaskT>
[[nodiscard]] IoStatus io_read_some(
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
        if (TaskT::Runtime::io_submit_read_at(
                thread,
                fd,
                data,
                size,
                detail::io_current_offset,
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
        const ssize_t n = ::read(fd, data, size);
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

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_readv_some(
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
        if (TaskT::Runtime::io_submit_readv_at(
                thread,
                fd,
                iov,
                iov_count,
                detail::io_current_offset,
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
        const ssize_t n = ::readv(fd, iov, iov_count);
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
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_read_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    void* data,
    std::size_t size,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_read_at(thread, fd, data, size, offset, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_readv_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    std::uint64_t offset,
    IoOpState& state) noexcept {
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
    if (TaskT::Runtime::io_submit_readv_at(
            thread,
            fd,
            iov,
            iov_count,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_write_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_write_at(thread, fd, data, size, offset, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_read_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    void* data,
    std::size_t size,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_read_fixed_file_at(
            thread,
            file_index,
            data,
            size,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_write_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const void* data,
    std::size_t size,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_write_fixed_file_at(
            thread,
            file_index,
            data,
            size,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_readv_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const iovec* iov,
    int iov_count,
    std::uint64_t offset,
    IoOpState& state) noexcept {
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

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_readv_fixed_file_at(
            thread,
            file_index,
            iov,
            iov_count,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_writev_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const iovec* iov,
    int iov_count,
    std::uint64_t offset,
    IoOpState& state) noexcept {
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

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_writev_fixed_file_at(
            thread,
            file_index,
            iov,
            iov_count,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_fsync_fixed_file(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    IoOpState& state,
    std::uint32_t flags = 0) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_fsync_fixed_file(
            thread,
            file_index,
            flags,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

#if !defined(_WIN32)
template <typename TaskT>
[[nodiscard]] IoStatus io_read_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    void* data,
    std::size_t size,
    std::uint64_t offset,
    std::uint16_t buffer_index,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_read_fixed_file_at(
            thread,
            file_index,
            data,
            size,
            offset,
            buffer_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_read_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    IoFixedBuffer buffer,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    return io_read_fixed_file_at(
        task,
        thread,
        file_index,
        buffer.data,
        buffer.size,
        offset,
        buffer.index,
        state);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_write_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    const void* data,
    std::size_t size,
    std::uint64_t offset,
    std::uint16_t buffer_index,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_write_fixed_file_at(
            thread,
            file_index,
            data,
            size,
            offset,
            buffer_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_write_fixed_file_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int file_index,
    IoFixedBuffer buffer,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    return io_write_fixed_file_at(
        task,
        thread,
        file_index,
        buffer.data,
        buffer.size,
        offset,
        buffer.index,
        state);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_read_fixed_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    void* data,
    std::size_t size,
    std::uint64_t offset,
    std::uint16_t buffer_index,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_read_fixed_at(
            thread,
            fd,
            data,
            size,
            offset,
            buffer_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_read_fixed_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    IoFixedBuffer buffer,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    return io_read_fixed_at(
        task,
        thread,
        fd,
        buffer.data,
        buffer.size,
        offset,
        buffer.index,
        state);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_write_fixed_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    std::uint64_t offset,
    std::uint16_t buffer_index,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (size == 0U) {
        return IoStatus::ready(0);
    }
    if (data == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_write_fixed_at(
            thread,
            fd,
            data,
            size,
            offset,
            buffer_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_write_fixed_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    IoFixedBuffer buffer,
    std::uint64_t offset,
    IoOpState& state) noexcept {
    return io_write_fixed_at(
        task,
        thread,
        fd,
        buffer.data,
        buffer.size,
        offset,
        buffer.index,
        state);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_writev_at(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    std::uint64_t offset,
    IoOpState& state) noexcept {
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
    if (TaskT::Runtime::io_submit_writev_at(
            thread,
            fd,
            iov,
            iov_count,
            offset,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}
#endif

template <typename TaskT>
[[nodiscard]] IoStatus io_fsync(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint32_t flags,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_fsync(thread, fd, flags, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_openat(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    int flags,
    std::uint32_t mode,
    int* opened_fd,
    IoOpState& state) noexcept {
    if (opened_fd == nullptr || path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    *opened_fd = -1;

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

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_openat(
            thread,
            dir_fd,
            path,
            flags,
            mode,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_openat_direct(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    int flags,
    std::uint32_t mode,
    int file_index,
    IoFixedFile<typename TaskT::Thread>* opened_file,
    IoOpState& state) noexcept {
    if (path == nullptr || opened_file == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (file_index < 0) {
        return IoStatus::failed(EBADF);
    }

    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (!completion.ready()) {
            return completion;
        }
        opened_file->reset(thread, file_index);
        return IoStatus::ready(0);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{file_index, 0, 0, 0};
    if (TaskT::Runtime::io_submit_openat_direct(
            thread,
            dir_fd,
            path,
            flags,
            mode,
            file_index,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_close(
    TaskT& task,
    typename TaskT::Thread thread,
    UniqueFd& fd,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    const int raw_fd = fd.get();
    if (raw_fd < 0) {
        return IoStatus::failed(EBADF);
    }

    state.wait = IoResult{raw_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_close(thread, raw_fd, &task, &state.wait)) {
        static_cast<void>(fd.release());
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_statx(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    int flags,
    std::uint32_t mask,
    struct statx* output,
    IoOpState& state) noexcept {
    if (path == nullptr || output == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_statx(
            thread,
            dir_fd,
            path,
            flags,
            mask,
            output,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_fallocate(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    int mode,
    std::uint64_t offset,
    std::uint64_t length,
    IoOpState& state) noexcept {
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);
    if (length == 0U) {
        return IoStatus::ready(0);
    }

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_fallocate(
            thread,
            fd,
            mode,
            offset,
            length,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_renameat(
    TaskT& task,
    typename TaskT::Thread thread,
    int old_dir_fd,
    const char* old_path,
    int new_dir_fd,
    const char* new_path,
    std::uint32_t flags,
    IoOpState& state) noexcept {
    if (old_path == nullptr || new_path == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{old_dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_renameat(
            thread,
            old_dir_fd,
            old_path,
            new_dir_fd,
            new_path,
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
[[nodiscard]] IoStatus io_unlinkat(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    int flags,
    IoOpState& state) noexcept {
    if (path == nullptr) {
        return IoStatus::failed(EINVAL);
    }

    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_unlinkat(
            thread,
            dir_fd,
            path,
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
[[nodiscard]] IoStatus io_write_some(
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
        if (TaskT::Runtime::io_submit_write_at(
                thread,
                fd,
                data,
                size,
                detail::io_current_offset,
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
        const ssize_t n = ::write(fd, data, size);
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
[[nodiscard]] IoStatus io_writev_some(
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
        if (TaskT::Runtime::io_submit_writev_at(
                thread,
                fd,
                iov,
                iov_count,
                detail::io_current_offset,
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
        const ssize_t n = ::writev(fd, iov, iov_count);
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
