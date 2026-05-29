#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "af/task.hpp"

#if !defined(_WIN32)
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace af {

enum class IoStep : std::uint8_t {
    Ready,
    Pending,
    Closed,
    Failed,
};

struct IoStatus {
    IoStep step{IoStep::Failed};
    std::size_t bytes{0};
    int error{0};

    [[nodiscard]] static IoStatus ready(std::size_t byte_count) noexcept {
        return {IoStep::Ready, byte_count, 0};
    }

    [[nodiscard]] static IoStatus make_pending() noexcept {
        return {IoStep::Pending, 0, 0};
    }

    [[nodiscard]] static IoStatus make_closed() noexcept {
        return {IoStep::Closed, 0, 0};
    }

    [[nodiscard]] static IoStatus failed(int error_code) noexcept {
        return {IoStep::Failed, 0, error_code == 0 ? EIO : error_code};
    }

    [[nodiscard]] bool ready() const noexcept {
        return step == IoStep::Ready;
    }

    [[nodiscard]] bool pending() const noexcept {
        return step == IoStep::Pending;
    }

    [[nodiscard]] bool closed() const noexcept {
        return step == IoStep::Closed;
    }

    [[nodiscard]] bool failed() const noexcept {
        return step == IoStep::Failed;
    }
};

struct IoOpState {
    IoResult wait{};
    bool waiting{false};

    void reset() noexcept {
        wait = IoResult{};
        waiting = false;
    }
};

namespace detail {

[[nodiscard]] inline bool io_would_block(int error) noexcept {
    return error == EAGAIN
#if EWOULDBLOCK != EAGAIN
        || error == EWOULDBLOCK
#endif
        ;
}

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
        return IoStatus::make_pending();
    }

    state.waiting = false;
    return IoStatus::failed(state.wait.error == 0 ? EINVAL : state.wait.error);
}

} // namespace detail

template <typename TaskT>
[[nodiscard]] IoStatus io_read_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    void* data,
    std::size_t size,
    IoOpState& state) noexcept {
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
    state.waiting = false;
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

template <typename TaskT>
[[nodiscard]] IoStatus io_write_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    IoOpState& state) noexcept {
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
    state.waiting = false;
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

template <typename TaskT>
[[nodiscard]] IoStatus io_recv_from_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    void* data,
    std::size_t size,
    sockaddr* address,
    socklen_t* address_size,
    IoOpState& state) noexcept {
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
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    state.waiting = false;
    for (;;) {
        const ssize_t n = ::recvfrom(fd, data, size, 0, address, address_size);
        if (n >= 0) {
            return IoStatus::ready(static_cast<std::size_t>(n));
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
[[nodiscard]] IoStatus io_send_to_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const void* data,
    std::size_t size,
    const sockaddr* address,
    socklen_t address_size,
    IoOpState& state) noexcept {
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
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    state.waiting = false;
    for (;;) {
        const ssize_t n = ::sendto(fd, data, size, 0, address, address_size);
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

} // namespace af
