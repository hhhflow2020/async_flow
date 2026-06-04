#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_select_backend.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

#if AF_DETAIL_HAS_SELECT
[[nodiscard]] inline bool set_select_fd_nonblocking_cloexec(int fd) noexcept {
    int flags = ::fcntl(fd, F_GETFD, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return false;
    }

    flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

[[nodiscard]] inline bool write_select_wake_byte(int fd) noexcept {
    const std::uint8_t value = 1;
    for (;;) {
        const ssize_t written = ::write(fd, &value, sizeof(value));
        if (written == static_cast<ssize_t>(sizeof(value))) {
            return true;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true;
        }
        return false;
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::native_io_backend_available() const noexcept {
    return io_select_wake_read_fd_ >= 0 && io_select_wake_write_fd_ >= 0;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::notify_native_io_backend() noexcept {
    if (!native_io_backend_available()) {
        return false;
    }

    bool expected = false;
    if (io_wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
        if (!detail::write_select_wake_byte(io_select_wake_write_fd_)) {
            io_wake_pending_.store(false, std::memory_order_relaxed);
            return false;
        }
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::init_native_io_backend() noexcept {
    if (!native_io_thread()) {
        return false;
    }
    if (native_io_backend_available()) {
        return true;
    }
    reserve_native_io_wait_storage();

    int pipe_fds[2]{-1, -1};
    if (::pipe(pipe_fds) != 0) {
        return false;
    }
    io_select_wake_read_fd_ = pipe_fds[0];
    io_select_wake_write_fd_ = pipe_fds[1];

    if (!select_fd_supported(io_select_wake_read_fd_) ||
        !select_fd_supported(io_select_wake_write_fd_) ||
        !detail::set_select_fd_nonblocking_cloexec(io_select_wake_read_fd_) ||
        !detail::set_select_fd_nonblocking_cloexec(io_select_wake_write_fd_)) {
        close_native_io_backend();
        return false;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_native_io_backend() noexcept {
    clear_io_waits();
    for (auto &entry : net_channels_) {
        if (entry.second != nullptr) {
            entry.second->active = false;
            entry.second->interests = 0;
            entry.second->backend_token = nullptr;
        }
    }
    net_channels_.clear();
    if (io_select_wake_read_fd_ >= 0) {
        ::close(io_select_wake_read_fd_);
        io_select_wake_read_fd_ = -1;
    }
    if (io_select_wake_write_fd_ >= 0) {
        ::close(io_select_wake_write_fd_);
        io_select_wake_write_fd_ = -1;
    }
    io_wake_pending_.store(false, std::memory_order_relaxed);
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_io_waits() noexcept {
    for (auto &entry : io_waits_) {
        if (entry.second.read != nullptr) {
            io_wait_pool_.destroy(entry.second.read);
        }
        if (entry.second.write != nullptr && entry.second.write != entry.second.read) {
            io_wait_pool_.destroy(entry.second.write);
        }
    }
    io_waits_.clear();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::reserve_native_io_wait_storage() noexcept {
    try {
        if constexpr (io_wait_reserve != 0U) {
            io_waits_.reserve(io_wait_reserve);
            io_wait_pool_.reserve_slots(io_wait_reserve);
        }
    } catch (...) {
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::drain_select_wake() noexcept {
    std::array<std::uint8_t, 64> buffer{};
    for (;;) {
        const ssize_t n = ::read(io_select_wake_read_fd_, buffer.data(), buffer.size());
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    io_wake_pending_.store(false, std::memory_order_relaxed);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::select_fd_supported(int fd) noexcept {
    return fd >= 0 && fd < FD_SETSIZE;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::select_io_events(int fd, fd_set &readfds, fd_set &writefds,
                                              fd_set &exceptfds) noexcept {
    std::uint32_t events = 0;
    if (FD_ISSET(fd, &readfds)) {
        events |= io_readable;
    }
    if (FD_ISSET(fd, &writefds)) {
        events |= io_writable;
    }
    if (FD_ISSET(fd, &exceptfds)) {
        events |= io_error;
    }
    return events;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::select_net_events(int fd, fd_set &readfds, fd_set &writefds,
                                               fd_set &exceptfds) noexcept {
    std::uint32_t events = 0;
    if (FD_ISSET(fd, &readfds)) {
        events |= detail::net_io_readable;
    }
    if (FD_ISSET(fd, &writefds)) {
        events |= detail::net_io_writable;
    }
    if (FD_ISSET(fd, &exceptfds)) {
        events |= detail::net_io_error;
    }
    return events;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_native_io(int timeout_ms,
                                                               bool did_work) noexcept {
    if (!native_io_backend_available()) {
        return did_work;
    }
    if (timeout_ms == 0 && io_waits_.empty() && net_channels_.empty() &&
        !io_wake_pending_.load(std::memory_order_acquire)) {
        return did_work;
    }

    fd_set readfds;
    fd_set writefds;
    fd_set exceptfds;
    FD_ZERO(&readfds);
    FD_ZERO(&writefds);
    FD_ZERO(&exceptfds);

    int max_fd = io_select_wake_read_fd_;
    FD_SET(io_select_wake_read_fd_, &readfds);

    auto add_fd = [&](int fd, std::uint32_t events) noexcept {
        if (!select_fd_supported(fd)) {
            return;
        }
        if ((events & io_readable) != 0U || (events & detail::net_io_readable) != 0U) {
            FD_SET(fd, &readfds);
        }
        if ((events & io_writable) != 0U || (events & detail::net_io_writable) != 0U) {
            FD_SET(fd, &writefds);
        }
        FD_SET(fd, &exceptfds);
        max_fd = std::max(max_fd, fd);
    };

    for (const auto &entry : io_waits_) {
        std::uint32_t events = 0;
        if (entry.second.read != nullptr) {
            events |= entry.second.read->events;
        }
        if (entry.second.write != nullptr) {
            events |= entry.second.write->events;
        }
        add_fd(entry.first, events);
    }
    for (const auto &entry : net_channels_) {
        const detail::NetIoChannel *channel = entry.second;
        if (channel != nullptr && channel->active) {
            add_fd(channel->fd, channel->interests);
        }
    }

    timeval timeout{};
    timeval *timeout_ptr = nullptr;
    if (timeout_ms >= 0) {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = static_cast<suseconds_t>(timeout_ms % 1000) * 1000;
        timeout_ptr = &timeout;
    }

    const int count = ::select(max_fd + 1, &readfds, &writefds, &exceptfds, timeout_ptr);
    if (count <= 0) {
        return did_work;
    }

    if (FD_ISSET(io_select_wake_read_fd_, &readfds)) {
        drain_select_wake();
        did_work = true;
    }

    std::array<detail::NetIoChannel *, FD_SETSIZE> ready_channels{};
    std::array<std::uint32_t, FD_SETSIZE> ready_channel_events{};
    std::size_t ready_channel_count = 0;
    for (auto &entry : net_channels_) {
        detail::NetIoChannel *channel = entry.second;
        if (channel == nullptr || !channel->active || channel->on_event == nullptr ||
            !select_fd_supported(channel->fd)) {
            continue;
        }

        const std::uint32_t events = select_net_events(channel->fd, readfds, writefds, exceptfds);
        if (events != 0U && ready_channel_count < ready_channels.size()) {
            ready_channels[ready_channel_count] = channel;
            ready_channel_events[ready_channel_count] = events;
            ++ready_channel_count;
        }
    }

    for (auto it = io_waits_.begin(); it != io_waits_.end();) {
        auto current = it++;
        const int fd = current->first;
        IoWaitEntry &entry = current->second;
        if (!select_fd_supported(fd)) {
            continue;
        }

        const std::uint32_t ready_events = select_io_events(fd, readfds, writefds, exceptfds);
        if (ready_events == 0U) {
            continue;
        }

        std::array<IoWaitRegistration *, 2> completed{};
        std::size_t completed_count = 0;
        auto collect_ready = [&](IoWaitRegistration *registration) {
            if (!io_wait_registration_uses_native_backend(registration) ||
                !io_wait_registration_ready(*registration, ready_events)) {
                return;
            }
            if (completed_count == 0U || completed[0] != registration) {
                completed[completed_count++] = registration;
            }
        };
        collect_ready(entry.read);
        collect_ready(entry.write);
        if (completed_count == 0U) {
            continue;
        }

        for (std::size_t completed_index = 0; completed_index < completed_count;
             ++completed_index) {
            IoWaitRegistration *registration = completed[completed_index];
            remove_io_wait_registration(entry, registration);
            registration->result->fd = fd;
            registration->result->events = ready_events;
            registration->result->error = 0;
            enqueue_pending_blocking(index_, registration->task);
            io_wait_pool_.destroy(registration);
        }
        if (io_wait_entry_empty(entry)) {
            io_waits_.erase(current);
        }
        did_work = true;
    }

    for (std::size_t i = 0; i < ready_channel_count; ++i) {
        detail::NetIoChannel *channel = ready_channels[i];
        if (channel != nullptr && channel->active && channel->on_event != nullptr) {
            channel->on_event(channel->owner, ready_channel_events[i]);
            did_work = true;
        }
    }
    return did_work;
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::update_net_channel_interest(detail::NetIoChannel *channel,
                                                              std::uint32_t events) noexcept {
    if (!native_io_backend_available() || channel == nullptr || !select_fd_supported(channel->fd)) {
        return false;
    }

    if ((events & (detail::net_io_readable | detail::net_io_writable)) == 0U) {
        channel->active = false;
        channel->interests = 0;
        channel->backend_token = nullptr;
        return true;
    }

    channel->active = true;
    channel->interests = events;
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                                     IoResult *result) noexcept {
    const bool unsupported_events = (events & (io_readable | io_writable)) == 0U;
    auto existing = io_waits_.find(fd);
    if (!native_io_backend_available() || !select_fd_supported(fd) || events == 0U ||
        unsupported_events ||
        (existing != io_waits_.end() && io_wait_events_conflict(existing->second, events)) ||
        net_channels_.find(fd) != net_channels_.end()) {
        int error = 0;
        if (fd < 0) {
            error = EBADF;
        } else if (!select_fd_supported(fd)) {
            error = EINVAL;
        } else if (events == 0U || unsupported_events) {
            error = EINVAL;
        } else if (!native_io_backend_available()) {
            error = ENOSYS;
        } else {
            error = EALREADY;
        }
        detail::set_io_result_error(*result, fd, error);
        return false;
    }

    IoWaitRegistration *registration = nullptr;
    try {
        registration = io_wait_pool_.create();
        registration->fd = fd;
        registration->events = events;
        registration->task = task;
        registration->result = result;
        auto [it, inserted] = io_waits_.try_emplace(fd);
        static_cast<void>(inserted);
        add_io_wait_registration(it->second, registration);
    } catch (...) {
        if (registration != nullptr) {
            io_wait_pool_.destroy(registration);
        }
        detail::set_io_result_error(*result, fd, ENOMEM);
        return false;
    }

    *result = IoResult{fd, 0, 0};
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::cancel_native_io_wait(IoOpState &state) noexcept {
    if (!native_io_backend_available()) {
        detail::set_io_result_error(state.wait, state.wait.fd, ENOSYS);
        return false;
    }

    const int fd = state.wait.fd;
    auto it = io_waits_.find(fd);
    IoWaitRegistration *registration =
        it == io_waits_.end() ? nullptr : find_io_wait_registration(it->second, &state.wait);
    if (fd < 0 || it == io_waits_.end() || registration == nullptr) {
        detail::set_io_result_error(state.wait, fd, ENOENT);
        return false;
    }

    remove_io_wait_registration(it->second, registration);
    if (io_wait_entry_empty(it->second)) {
        io_waits_.erase(it);
    }

    detail::set_io_result_error(state.wait, fd, ECANCELED);
    if (registration->task != running_task_) {
        enqueue_pending_blocking(index_, registration->task);
    }
    io_wait_pool_.destroy(registration);
    return true;
}
#endif

} // namespace af::detail
