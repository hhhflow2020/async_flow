#if !defined(AF_IO_FILE_FRAGMENT_INCLUDE)
#error "io_file_lifecycle_fragment.hpp is an io_file implementation fragment"
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

