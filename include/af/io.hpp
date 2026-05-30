#pragma once

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <type_traits>
#include <utility>

#include "af/task.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

#if defined(__linux__)
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/sendfile.h>
#include <sys/timerfd.h>
#endif

#if !defined(__linux__)
struct statx;
#endif

namespace af {

#if defined(_WIN32)
using IoOffset = std::int64_t;
#else
using IoOffset = off_t;
#endif

#if !defined(_WIN32)
struct IoFixedBuffer {
    void* data{nullptr};
    std::size_t size{0};
    std::uint16_t index{0};
};
#endif

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

class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }

    ~UniqueFd() {
        reset();
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] int release() noexcept {
        return std::exchange(fd_, -1);
    }

    void reset(int fd = -1) noexcept {
        if (fd_ == fd) {
            return;
        }
#if !defined(_WIN32)
        if (fd_ >= 0) {
            ::close(fd_);
        }
#endif
        fd_ = fd;
    }

private:
    int fd_{-1};
};

namespace detail {

[[nodiscard]] inline bool io_would_block(int error) noexcept {
    return error == EAGAIN
#if EWOULDBLOCK != EAGAIN
        || error == EWOULDBLOCK
#endif
        ;
}

[[nodiscard]] inline int io_no_signal_flag() noexcept {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

[[nodiscard]] inline int io_default_accept_flags() noexcept {
    int flags = 0;
#if defined(SOCK_NONBLOCK)
    flags |= SOCK_NONBLOCK;
#endif
#if defined(SOCK_CLOEXEC)
    flags |= SOCK_CLOEXEC;
#endif
    return flags;
}

#if defined(__linux__)
[[nodiscard]] inline int io_default_eventfd_flags() noexcept {
    int flags = 0;
#if defined(EFD_NONBLOCK)
    flags |= EFD_NONBLOCK;
#endif
#if defined(EFD_CLOEXEC)
    flags |= EFD_CLOEXEC;
#endif
    return flags;
}

[[nodiscard]] inline int io_default_timerfd_flags() noexcept {
    int flags = 0;
#if defined(TFD_NONBLOCK)
    flags |= TFD_NONBLOCK;
#endif
#if defined(TFD_CLOEXEC)
    flags |= TFD_CLOEXEC;
#endif
    return flags;
}

[[nodiscard]] inline timespec io_timespec_from_duration(
    std::chrono::nanoseconds duration) noexcept {
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
    duration -= seconds;
    return timespec{
        static_cast<time_t>(seconds.count()),
        static_cast<long>(duration.count())};
}
#endif

[[nodiscard]] inline bool io_connect_in_progress(int error) noexcept {
    return error == EINPROGRESS || error == EALREADY || error == EINTR ||
           error == EAGAIN
#if EWOULDBLOCK != EAGAIN
        || error == EWOULDBLOCK
#endif
        ;
}

[[nodiscard]] inline int io_socket_connect_error(int fd) noexcept {
    int error = 0;
    socklen_t error_size = sizeof(error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
        return errno == 0 ? EIO : errno;
    }
    return error;
}

[[nodiscard]] inline bool io_apply_accepted_flags(int fd, int flags, int& error) noexcept {
    error = 0;
#if defined(SOCK_NONBLOCK)
    if ((flags & SOCK_NONBLOCK) != 0) {
        const int current = ::fcntl(fd, F_GETFL, 0);
        if (current < 0 || ::fcntl(fd, F_SETFL, current | O_NONBLOCK) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
    }
#else
    static_cast<void>(flags);
#endif
#if defined(SOCK_CLOEXEC)
    if ((flags & SOCK_CLOEXEC) != 0) {
        const int current = ::fcntl(fd, F_GETFD, 0);
        if (current < 0 || ::fcntl(fd, F_SETFD, current | FD_CLOEXEC) != 0) {
            error = errno == 0 ? EIO : errno;
            return false;
        }
    }
#endif
    return true;
}

#if defined(__linux__)
[[nodiscard]] inline bool io_fd_can_wait(int fd) noexcept {
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
        return false;
    }
    return S_ISSOCK(status.st_mode) || S_ISFIFO(status.st_mode) || S_ISCHR(status.st_mode);
}

[[nodiscard]] inline bool io_poll_ready(int fd, short events) noexcept {
    pollfd descriptor{fd, events, 0};
    for (;;) {
        const int ready = ::poll(&descriptor, 1, 0);
        if (ready > 0) {
            return (descriptor.revents & (events | POLLERR | POLLHUP | POLLNVAL)) != 0;
        }
        if (ready == 0) {
            return false;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}
#endif

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

} // namespace detail

#if defined(__linux__)
[[nodiscard]] inline UniqueFd make_eventfd(
    unsigned int init_value = 0,
    int flags = detail::io_default_eventfd_flags()) noexcept {
    return UniqueFd(::eventfd(init_value, flags));
}

[[nodiscard]] inline bool write_eventfd(
    int fd,
    std::uint64_t value,
    int& error) noexcept {
    error = 0;
    if (fd < 0) {
        error = EBADF;
        return false;
    }
    if (value == std::numeric_limits<std::uint64_t>::max()) {
        error = EINVAL;
        return false;
    }

    for (;;) {
        const ssize_t n = ::write(fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            return true;
        }
        if (n >= 0) {
            error = EIO;
            return false;
        }

        error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return false;
    }
}

[[nodiscard]] inline UniqueFd make_timerfd(
    clockid_t clock_id = CLOCK_MONOTONIC,
    int flags = detail::io_default_timerfd_flags()) noexcept {
    return UniqueFd(::timerfd_create(clock_id, flags));
}

[[nodiscard]] inline bool arm_timerfd(
    int fd,
    std::chrono::nanoseconds initial,
    std::chrono::nanoseconds interval,
    int& error) noexcept {
    error = 0;
    if (fd < 0) {
        error = EBADF;
        return false;
    }
    if (initial.count() < 0 || interval.count() < 0) {
        error = EINVAL;
        return false;
    }

    itimerspec spec{};
    if (initial.count() != 0) {
        spec.it_value = detail::io_timespec_from_duration(initial);
    }
    if (interval.count() != 0) {
        spec.it_interval = detail::io_timespec_from_duration(interval);
    }
    if (::timerfd_settime(fd, 0, &spec, nullptr) != 0) {
        error = errno == 0 ? EIO : errno;
        return false;
    }
    return true;
}

[[nodiscard]] inline bool arm_timerfd_after(
    int fd,
    std::chrono::nanoseconds delay,
    int& error) noexcept {
    if (delay.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, delay, std::chrono::nanoseconds{0}, error);
}

[[nodiscard]] inline bool arm_timerfd_every(
    int fd,
    std::chrono::nanoseconds interval,
    int& error) noexcept {
    if (interval.count() <= 0) {
        error = EINVAL;
        return false;
    }
    return arm_timerfd(fd, interval, interval, error);
}

[[nodiscard]] inline bool disarm_timerfd(int fd, int& error) noexcept {
    return arm_timerfd(
        fd,
        std::chrono::nanoseconds{0},
        std::chrono::nanoseconds{0},
        error);
}
#endif

struct IoDeadline {
    std::chrono::nanoseconds delay{0};
    UniqueFd timer{};
    IoOpState wait{};
    std::uint64_t expirations{0};
    bool armed{false};
    bool cancel_pending{false};

    void set_after(std::chrono::nanoseconds timeout) noexcept {
        delay = timeout;
        reset_runtime();
    }

    void reset_runtime() noexcept {
        wait.reset();
        expirations = 0;
        armed = false;
        cancel_pending = false;
    }

    void reset() noexcept {
        reset_runtime();
        timer.reset();
        delay = std::chrono::nanoseconds{0};
    }

    [[nodiscard]] bool configured() const noexcept {
        return delay.count() > 0;
    }
};

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
    detail::clear_waiting(state);
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

    detail::clear_waiting(state);
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

#if !defined(_WIN32)
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
    detail::clear_waiting(state);
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

    detail::clear_waiting(state);
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
    static_cast<void>(address);
    static_cast<void>(address_size);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    if (detail::waiting_for_completion(state)) {
        const IoStatus completion = detail::completed_uring_status(state);
        if (completion.failed() && detail::io_would_block(completion.error)) {
            return detail::arm_io_wait(task, thread, fd, io_readable, state);
        }
        return completion;
    }
    const bool resumed_from_readiness = state.waiting && state.wait_kind == IoWaitKind::Readiness;
    detail::clear_waiting(state);
    if (!resumed_from_readiness && TaskT::Runtime::io_uring_backend_available(thread)) {
        state.wait = IoResult{fd, 0, 0, 0};
        if (TaskT::Runtime::io_submit_recvmsg(
                thread,
                fd,
                data,
                size,
                address,
                address_size,
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
    static_cast<void>(address);
    static_cast<void>(address_size);
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
        if (TaskT::Runtime::io_submit_sendmsg(
                thread,
                fd,
                data,
                size,
                address,
                address_size,
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
        const ssize_t n = ::sendto(
            fd,
            data,
            size,
            detail::io_no_signal_flag(),
            address,
            address_size);
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
[[nodiscard]] IoStatus io_recvv_from_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    sockaddr* address,
    socklen_t* address_size,
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
                address,
                address_size,
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
    message.msg_name = address;
    message.msg_namelen = address_size == nullptr ? 0 : *address_size;
    message.msg_iov = const_cast<iovec*>(iov);
    message.msg_iovlen = static_cast<std::size_t>(iov_count);
    for (;;) {
        const ssize_t n = ::recvmsg(fd, &message, 0);
        if (n >= 0) {
            if (address_size != nullptr) {
                *address_size = message.msg_namelen;
            }
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
}

template <typename TaskT>
[[nodiscard]] IoStatus io_sendv_to_some(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    const iovec* iov,
    int iov_count,
    const sockaddr* address,
    socklen_t address_size,
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
                address,
                address_size,
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
    message.msg_name = const_cast<sockaddr*>(address);
    message.msg_namelen = address_size;
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

template <typename TaskT>
[[nodiscard]] IoStatus io_wait_eventfd(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint64_t* value,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (value == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    detail::clear_waiting(state);
    for (;;) {
        std::uint64_t counter = 0;
        const ssize_t n = ::read(fd, &counter, sizeof(counter));
        if (n == static_cast<ssize_t>(sizeof(counter))) {
            *value = counter;
            return IoStatus::ready(sizeof(counter));
        }
        if (n >= 0) {
            return IoStatus::failed(EIO);
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
[[nodiscard]] IoStatus io_wait_timerfd(
    TaskT& task,
    typename TaskT::Thread thread,
    int fd,
    std::uint64_t* expirations,
    IoOpState& state) noexcept {
    if (detail::cancelled_wait_ready(state)) [[unlikely]] {
        return detail::consume_cancelled_wait(state);
    }
    if (expirations == nullptr) {
        return IoStatus::failed(EINVAL);
    }

#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(fd);
    static_cast<void>(state);
    return IoStatus::failed(ENOSYS);
#else
    detail::clear_waiting(state);
    for (;;) {
        std::uint64_t value = 0;
        const ssize_t n = ::read(fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            *expirations = value;
            return IoStatus::ready(sizeof(value));
        }
        if (n >= 0) {
            return IoStatus::failed(EIO);
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
[[nodiscard]] IoStatus arm_io_timeout(
    TaskT& task,
    typename TaskT::Thread thread,
    IoDeadline& deadline,
    IoOpState& io_state) noexcept {
#if !defined(__linux__)
    static_cast<void>(task);
    static_cast<void>(thread);
    static_cast<void>(deadline);
    static_cast<void>(io_state);
    return IoStatus::failed(ENOSYS);
#else
    if (!deadline.configured()) {
        return IoStatus::failed(EINVAL);
    }

    if (deadline.cancel_pending) {
        if (!detail::io_wait_result_ready(io_state)) {
            return IoStatus::make_pending();
        }
        detail::clear_waiting(io_state);
        deadline.reset_runtime();
        return IoStatus::failed(ETIMEDOUT);
    }

    if (deadline.armed && detail::io_wait_result_ready(io_state)) {
        if (detail::io_wait_result_ready(deadline.wait)) {
            std::uint64_t ignored = 0;
            static_cast<void>(io_wait_timerfd(
                task,
                thread,
                deadline.timer.get(),
                &ignored,
                deadline.wait));
        } else if (deadline.wait.waiting) {
            static_cast<void>(TaskT::Runtime::cancel_io(thread, deadline.wait));
        }
        int error = 0;
        static_cast<void>(disarm_timerfd(deadline.timer.get(), error));
        deadline.reset_runtime();
        return IoStatus::ready(0);
    }

    if (deadline.armed && detail::io_wait_result_ready(deadline.wait)) {
        const IoStatus timeout = io_wait_timerfd(
            task,
            thread,
            deadline.timer.get(),
            &deadline.expirations,
            deadline.wait);
        if (!timeout.ready()) {
            deadline.reset_runtime();
            return timeout.failed() ? timeout : IoStatus::failed(EIO);
        }

        const IoWaitKind io_kind = io_state.wait_kind;
        if (!TaskT::Runtime::cancel_io(thread, io_state)) {
            const int error = io_state.wait.error == 0 ? EIO : io_state.wait.error;
            deadline.reset_runtime();
            return IoStatus::failed(error);
        }

        if (io_kind == IoWaitKind::Completion) {
            deadline.cancel_pending = true;
            deadline.armed = false;
            return IoStatus::make_pending();
        }

        detail::clear_waiting(io_state);
        deadline.reset_runtime();
        return IoStatus::failed(ETIMEDOUT);
    }

    if (deadline.armed) {
        return IoStatus::make_pending();
    }
    if (!io_state.waiting) {
        return IoStatus::failed(EINVAL);
    }

    if (!deadline.timer) {
        deadline.timer = make_timerfd();
        if (!deadline.timer) {
            return IoStatus::failed(errno == 0 ? EIO : errno);
        }
    }

    int error = 0;
    if (!arm_timerfd_after(deadline.timer.get(), deadline.delay, error)) {
        return IoStatus::failed(error);
    }

    deadline.wait.reset();
    deadline.expirations = 0;
    const IoStatus status = io_wait_timerfd(
        task,
        thread,
        deadline.timer.get(),
        &deadline.expirations,
        deadline.wait);
    if (status.pending()) {
        deadline.armed = true;
    }
    return status;
#endif
}

template <typename ThreadT>
class IoDescriptor {
public:
    constexpr IoDescriptor() noexcept = default;
    constexpr IoDescriptor(ThreadT thread, int fd) noexcept : thread_(thread), fd_(fd) {}

    [[nodiscard]] constexpr ThreadT thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] constexpr int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] constexpr bool valid() const noexcept {
        return fd_ >= 0;
    }

    constexpr void reset(ThreadT thread, int fd) noexcept {
        thread_ = thread;
        fd_ = fd;
    }

protected:
    ThreadT thread_{};
    int fd_{-1};
};

template <typename ThreadT>
class IoFile : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus read_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_some(task, this->thread_, this->fd_, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_readv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_writev_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus read_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_at(task, this->thread_, this->fd_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_fixed_at(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_fixed_at(task, this->thread_, this->fd_, buffer, offset, state);
    }
#endif

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_readv_at(task, this->thread_, this->fd_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_at(task, this->thread_, this->fd_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_fixed_at(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_fixed_at(task, this->thread_, this->fd_, buffer, offset, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_writev_at(task, this->thread_, this->fd_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus fsync(
        TaskT& task,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_fsync(task, this->thread_, this->fd_, flags, state);
    }
};

template <typename ThreadT>
class IoStream : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recv_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_send_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendfile_some(
        TaskT& task,
        int file_fd,
        IoOffset* offset,
        std::size_t count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendfile_some(
            task,
            this->thread_,
            this->fd_,
            file_fd,
            offset,
            count,
            state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recvv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus connect(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_connect(task, this->thread_, this->fd_, address, address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return recv_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return recvv_some(task, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return send_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus writev_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return sendv_some(task, iov, iov_count, state);
    }
#endif
};

template <typename ThreadT>
class IoListener : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_some(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int* accepted_fd,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_some(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            accepted_fd,
            state,
            flags);
    }
};

template <typename ThreadT>
class IoDatagramSocket : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_from_some(
        TaskT& task,
        void* data,
        std::size_t size,
        sockaddr* address,
        socklen_t* address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recv_from_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_to_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_to_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_from_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        sockaddr* address,
        socklen_t* address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recvv_from_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_to_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_to_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }
#endif
};

template <typename ThreadT>
using TcpStream = IoStream<ThreadT>;

template <typename ThreadT>
using TcpListener = IoListener<ThreadT>;

template <typename ThreadT>
using UdpSocket = IoDatagramSocket<ThreadT>;

template <typename ThreadT>
class IoEvent : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* value,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoEvent thread type must match the task runtime thread type");
        return af::io_wait_eventfd(task, this->thread_, this->fd_, value, state);
    }
};

template <typename ThreadT>
class IoTimer : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* expirations,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoTimer thread type must match the task runtime thread type");
        return af::io_wait_timerfd(task, this->thread_, this->fd_, expirations, state);
    }
};

} // namespace af
