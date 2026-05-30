#pragma once

#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "af/io_common.hpp"

#if defined(__linux__)
#include <linux/openat2.h>
#endif

#if !defined(__linux__)
struct open_how;
#endif

namespace af {

template <typename TaskT>
[[nodiscard]] IoStatus io_openat2(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    const struct open_how* how,
    int* opened_fd,
    IoOpState& state) noexcept {
    if (opened_fd == nullptr || path == nullptr || how == nullptr) {
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
    if (TaskT::Runtime::io_submit_openat2(
            thread,
            dir_fd,
            path,
            how,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_mkdirat(
    TaskT& task,
    typename TaskT::Thread thread,
    int dir_fd,
    const char* path,
    std::uint32_t mode,
    IoOpState& state) noexcept {
    if (path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_mkdirat(thread, dir_fd, path, mode, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_symlinkat(
    TaskT& task,
    typename TaskT::Thread thread,
    const char* target,
    int new_dir_fd,
    const char* link_path,
    IoOpState& state) noexcept {
    if (target == nullptr || link_path == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{new_dir_fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_symlinkat(
            thread,
            target,
            new_dir_fd,
            link_path,
            &task,
            &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename TaskT>
[[nodiscard]] IoStatus io_linkat(
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
    if (TaskT::Runtime::io_submit_linkat(
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
[[nodiscard]] IoStatus io_ftruncate(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint64_t length,
    IoOpState& state) noexcept {
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (detail::waiting_for_completion(state)) {
        return detail::completed_uring_status(state);
    }
    detail::clear_waiting(state);

    state.wait = IoResult{fd, 0, 0, 0};
    if (TaskT::Runtime::io_submit_ftruncate(thread, fd, length, &task, &state.wait)) {
        state.waiting = true;
        state.wait_kind = IoWaitKind::Completion;
        return IoStatus::make_pending();
    }
    return IoStatus::failed(state.wait.error == 0 ? ENOSYS : state.wait.error);
}

template <typename ThreadT>
class IoDirectory {
public:
    constexpr IoDirectory() noexcept = default;
    constexpr IoDirectory(ThreadT thread, int fd) noexcept : thread_(thread), fd_(fd) {}

    void reset(ThreadT thread, int fd) noexcept {
        thread_ = thread;
        fd_ = fd;
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] ThreadT thread() const noexcept {
        return thread_;
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus openat2(
        TaskT& task,
        const char* path,
        const struct open_how* how,
        int* opened_fd,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_openat2(task, thread_, fd_, path, how, opened_fd, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus mkdirat(
        TaskT& task,
        const char* path,
        std::uint32_t mode,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_mkdirat(task, thread_, fd_, path, mode, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus symlinkat(
        TaskT& task,
        const char* target,
        const char* link_path,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_symlinkat(task, thread_, target, fd_, link_path, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus linkat(
        TaskT& task,
        int old_dir_fd,
        const char* old_path,
        const char* new_path,
        std::uint32_t flags,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_linkat(task, thread_, old_dir_fd, old_path, fd_, new_path, flags, state);
    }

private:
    ThreadT thread_{};
    int fd_{-1};
};

} // namespace af
