#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_epoll_backend.hpp is internal"
#endif

namespace af::detail {

#if AF_DETAIL_HAS_EPOLL
template <typename RuntimeT, typename TraitsT>
std::uint64_t Executor<RuntimeT, TraitsT>::epoll_wait_token(int fd) noexcept {
    return static_cast<std::uint64_t>(static_cast<std::uint32_t>(fd)) << 2U;
}

template <typename RuntimeT, typename TraitsT>
std::uint64_t Executor<RuntimeT, TraitsT>::epoll_wake_token() noexcept {
    return 1U;
}

template <typename RuntimeT, typename TraitsT>
std::uint64_t
Executor<RuntimeT, TraitsT>::epoll_channel_token(detail::NetIoChannel *channel) noexcept {
    return (static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(channel)) & ~3ULL) | 2ULL;
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::epoll_token_is_wake(std::uint64_t token) noexcept {
    return token == epoll_wake_token();
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::epoll_token_is_channel(std::uint64_t token) noexcept {
    return (token & 3ULL) == 2ULL;
}

template <typename RuntimeT, typename TraitsT>
int Executor<RuntimeT, TraitsT>::epoll_token_fd(std::uint64_t token) noexcept {
    return static_cast<int>(static_cast<std::uint32_t>(token >> 2U));
}

template <typename RuntimeT, typename TraitsT>
detail::NetIoChannel *
Executor<RuntimeT, TraitsT>::epoll_token_channel(std::uint64_t token) noexcept {
    return reinterpret_cast<detail::NetIoChannel *>(static_cast<std::uintptr_t>(token & ~3ULL));
}

[[nodiscard]] inline bool write_epoll_wake_eventfd(int fd) noexcept {
    const std::uint64_t value = 1;
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
void Executor<RuntimeT, TraitsT>::drain_io_wake() noexcept {
    std::uint64_t value = 0;
    while (::read(io_wake_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
    io_wake_pending_.store(false, std::memory_order_relaxed);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::native_poll_events(std::uint32_t events) noexcept {
    std::uint32_t result = 0;
    if ((events & io_readable) != 0U) {
        result |= POLLIN;
    }
    if ((events & io_writable) != 0U) {
        result |= POLLOUT;
    }
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::io_events_from_poll(std::uint32_t events) noexcept {
    std::uint32_t result = 0;
    if ((events & (POLLIN | POLLPRI)) != 0U) {
        result |= io_readable;
    }
    if ((events & POLLOUT) != 0U) {
        result |= io_writable;
    }
    if ((events & (POLLERR | POLLNVAL)) != 0U) {
        result |= io_error;
    }
    if ((events & POLLHUP) != 0U) {
        result |= io_hangup;
    }
#ifdef POLLRDHUP
    if ((events & POLLRDHUP) != 0U) {
        result |= io_hangup;
    }
#endif
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::net_events_from_poll(std::uint32_t events) noexcept {
    std::uint32_t result = 0;
    if ((events & (POLLIN | POLLPRI)) != 0U) {
        result |= detail::net_io_readable;
    }
    if ((events & POLLOUT) != 0U) {
        result |= detail::net_io_writable;
    }
    if ((events & (POLLERR | POLLNVAL)) != 0U) {
        result |= detail::net_io_error;
    }
    if ((events & POLLHUP) != 0U) {
        result |= detail::net_io_hangup;
    }
#ifdef POLLRDHUP
    if ((events & POLLRDHUP) != 0U) {
        result |= detail::net_io_hangup;
    }
#endif
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::io_events_from_native(std::uint32_t events) noexcept {
    std::uint32_t result = 0;
    if ((events & EPOLLIN) != 0U) {
        result |= io_readable;
    }
    if ((events & EPOLLOUT) != 0U) {
        result |= io_writable;
    }
    if ((events & EPOLLERR) != 0U) {
        result |= io_error;
    }
    if ((events & EPOLLHUP) != 0U) {
        result |= io_hangup;
    }
#ifdef EPOLLRDHUP
    if ((events & EPOLLRDHUP) != 0U) {
        result |= io_hangup;
    }
#endif
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::native_io_backend_available() const noexcept {
    return io_epoll_fd_ >= 0;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::notify_native_io_backend() noexcept {
    if (io_epoll_fd_ < 0) {
        return false;
    }
    bool expected = false;
    if (io_wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
        if (!detail::write_epoll_wake_eventfd(io_wake_fd_)) {
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
    if (io_epoll_fd_ >= 0) {
        return true;
    }
    reserve_io_backend_storage();

    io_epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (io_epoll_fd_ < 0) {
        return false;
    }

    io_wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (io_wake_fd_ < 0) {
        close_native_io_backend();
        return false;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = epoll_wake_token();
    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
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
    if (io_wake_fd_ >= 0) {
        ::close(io_wake_fd_);
        io_wake_fd_ = -1;
    }
    if (io_epoll_fd_ >= 0) {
        ::close(io_epoll_fd_);
        io_epoll_fd_ = -1;
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
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_native_io(int timeout_ms,
                                                               bool did_work) noexcept {
    if (io_epoll_fd_ < 0) {
        return did_work;
    }
    if (timeout_ms == 0 && io_waits_.empty() && net_channels_.empty() &&
        !io_wake_pending_.load(std::memory_order_acquire)) {
        return did_work;
    }

    std::array<epoll_event, 64> events;
    const int count =
        ::epoll_wait(io_epoll_fd_, events.data(), static_cast<int>(events.size()), timeout_ms);
    if (count <= 0) {
        return did_work;
    }

    for (int i = 0; i < count; ++i) {
        const epoll_event &event = events[static_cast<std::size_t>(i)];
        const std::uint64_t token = event.data.u64;
        if (epoll_token_is_wake(token)) {
            drain_io_wake();
            if (poll_io_uring_completions()) {
                did_work = true;
            }
            did_work = true;
            continue;
        }
        if (epoll_token_is_channel(token)) {
            detail::NetIoChannel *channel = epoll_token_channel(token);
            if (channel != nullptr && channel->active && channel->on_event != nullptr) {
                channel->on_event(channel->owner, io_events_from_native(event.events));
                did_work = true;
            }
            continue;
        }

        const int fd = epoll_token_fd(token);
        auto it = io_waits_.find(fd);
        if (it == io_waits_.end()) {
            static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
            continue;
        }

        const std::uint32_t ready_events =
            io_events_from_native(events[static_cast<std::size_t>(i)].events);
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
        collect_ready(it->second.read);
        collect_ready(it->second.write);
        if (completed_count == 0U) {
            static_cast<void>(update_epoll_interest(fd, it->second));
            continue;
        }

        for (std::size_t completed_index = 0; completed_index < completed_count;
             ++completed_index) {
            IoWaitRegistration *registration = completed[completed_index];
            remove_io_wait_registration(it->second, registration);
            registration->result->fd = fd;
            registration->result->events = ready_events;
            registration->result->error = 0;
            enqueue_pending_blocking(index_, registration->task);
            io_wait_pool_.destroy(registration);
        }
        if (io_wait_entry_empty(it->second)) {
            io_waits_.erase(it);
            static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
        } else {
            static_cast<void>(update_epoll_interest(fd, it->second));
        }
        did_work = true;
    }
    return did_work;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::epoll_events_for_entry(const IoWaitEntry &entry) noexcept {
    std::uint32_t native_events = EPOLLERR | EPOLLHUP | EPOLLONESHOT;
    bool has_native_wait = false;
    if (io_wait_registration_uses_native_backend(entry.read)) {
        native_events |= EPOLLIN;
        has_native_wait = true;
    }
    if (io_wait_registration_uses_native_backend(entry.write)) {
        native_events |= EPOLLOUT;
        has_native_wait = true;
    }
    return has_native_wait ? native_events : 0U;
}

template <typename RuntimeT, typename TraitsT>
std::uint32_t
Executor<RuntimeT, TraitsT>::epoll_events_for_net_channel(const detail::NetIoChannel &channel,
                                                          std::uint32_t events) noexcept {
    static_cast<void>(channel);
    std::uint32_t native_events = EPOLLERR | EPOLLHUP;
#ifdef EPOLLRDHUP
    native_events |= EPOLLRDHUP;
#endif
    if ((events & detail::net_io_readable) != 0U) {
        native_events |= EPOLLIN;
    }
    if ((events & detail::net_io_writable) != 0U) {
        native_events |= EPOLLOUT;
    }
    return native_events;
}

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_uring_net_poll_unsupported(int result) noexcept {
    const int error = result < 0 ? -result : result;
    return error == EINVAL || error == EOPNOTSUPP || error == ENOSYS;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] typename Executor<RuntimeT, TraitsT>::IoUringPollSubmitResult
Executor<RuntimeT, TraitsT>::try_submit_io_uring_net_poll(detail::NetIoChannel *channel,
                                                          std::uint32_t events) noexcept {
    if (!io_uring_thread() || io_uring_fd_ < 0 || !io_uring_poll_add_available_) {
        return IoUringPollSubmitResult::Fallback;
    }
    if (channel == nullptr || channel->fd < 0 ||
        (events & (detail::net_io_readable | detail::net_io_writable)) == 0U) {
        return IoUringPollSubmitResult::Failed;
    }

    const std::uint32_t native_events = native_poll_events(events);
    if (native_events == 0U) {
        return IoUringPollSubmitResult::Failed;
    }

    IoUringOperation *operation = nullptr;
    try {
        operation = io_uring_op_pool_.create();
    } catch (...) {
        return IoUringPollSubmitResult::Failed;
    }

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        io_uring_op_pool_.destroy(operation);
        if (io_uring_fd_ < 0) {
            return IoUringPollSubmitResult::BackendClosed;
        }
        return IoUringPollSubmitResult::Fallback;
    }

    operation->task = nullptr;
    operation->result = nullptr;
    operation->prev = nullptr;
    operation->next = nullptr;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;
    operation->net_channel = channel;
    operation->complete_events = 0;
    operation->poll_flags = io_uring_net_poll_flags_;
    operation->direct_file_index = -1;
    operation->opcode = IORING_OP_POLL_ADD;
    operation->cancel_requested = false;
    operation->multishot = (operation->poll_flags & IORING_POLL_ADD_MULTI) != 0U;
    operation->poll_wait = false;
    operation->net_poll = true;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;

    track_io_uring_operation(operation);

    *sqe = io_uring_sqe{};
    sqe->opcode = IORING_OP_POLL_ADD;
    sqe->fd = channel->fd;
    sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
    sqe->poll32_events = native_events;
    sqe->len = operation->poll_flags;

    channel->backend_token = operation;
    channel->active = true;
    channel->interests = events;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return IoUringPollSubmitResult::Submitted;
        }
        fail_io_uring_backend(submit_error, operation);
        return IoUringPollSubmitResult::BackendClosed;
    }
    return IoUringPollSubmitResult::Submitted;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::detach_io_uring_net_poll(detail::NetIoChannel *channel) noexcept {
    if (channel == nullptr || channel->backend_token == nullptr) {
        return;
    }

    auto *operation = static_cast<IoUringOperation *>(channel->backend_token);
    channel->backend_token = nullptr;
    if (operation->net_channel == channel) {
        operation->net_channel = nullptr;
    }
    operation->cancel_requested = true;
    static_cast<void>(submit_io_uring_cancel(operation));
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::fallback_io_uring_net_poll_to_epoll(
    detail::NetIoChannel *channel) noexcept {
    if (channel == nullptr || channel->fd < 0) {
        return false;
    }
    auto it = net_channels_.find(channel->fd);
    if (it == net_channels_.end() || it->second != channel) {
        return false;
    }
    const std::uint32_t interests = channel->interests;
    channel->backend_token = nullptr;
    channel->active = false;
    if ((interests & (detail::net_io_readable | detail::net_io_writable)) == 0U) {
        return true;
    }
    return update_epoll_net_channel_interest(channel, interests);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::degrade_io_uring_net_poll_flags(
    std::uint32_t rejected_flags) noexcept {
    if ((rejected_flags & IORING_POLL_ADD_LEVEL) != 0U) {
        io_uring_net_poll_flags_ &= ~static_cast<std::uint32_t>(IORING_POLL_ADD_LEVEL);
        return true;
    }
    if ((rejected_flags & IORING_POLL_ADD_MULTI) != 0U) {
        io_uring_net_poll_flags_ &= ~static_cast<std::uint32_t>(IORING_POLL_ADD_MULTI);
        return true;
    }
    return false;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::complete_io_uring_net_poll(IoUringOperation *operation,
                                                             int result,
                                                             std::uint32_t cqe_flags) noexcept {
    detail::NetIoChannel *channel = operation->net_channel;
    const bool more =
        result >= 0 && (cqe_flags & IORING_CQE_F_MORE) != 0U && !operation->cancel_requested;

    if (!more) {
        untrack_io_uring_operation(operation);
        if (channel != nullptr && channel->backend_token == operation) {
            channel->backend_token = nullptr;
            channel->active = false;
        }
        operation->net_channel = nullptr;
    }

    if (operation->cancel_requested || channel == nullptr) {
        if (!more) {
            destroy_io_uring_operation(operation);
        }
        return;
    }

    auto it = net_channels_.find(channel->fd);
    if (it == net_channels_.end() || it->second != channel) {
        if (!more) {
            destroy_io_uring_operation(operation);
        }
        return;
    }

    if (result < 0 && io_uring_net_poll_unsupported(result)) {
        if (!more) {
            if (degrade_io_uring_net_poll_flags(operation->poll_flags) &&
                channel->interests != 0U) {
                const IoUringPollSubmitResult retry_result =
                    try_submit_io_uring_net_poll(channel, channel->interests);
                if (retry_result == IoUringPollSubmitResult::Submitted) {
                    destroy_io_uring_operation(operation);
                    return;
                }
            }
            static_cast<void>(fallback_io_uring_net_poll_to_epoll(channel));
            destroy_io_uring_operation(operation);
        }
        return;
    }

    std::uint32_t events = detail::net_io_error;
    if (result >= 0) {
        events = net_events_from_poll(static_cast<std::uint32_t>(result));
    }

    if (!more && channel->interests != 0U) {
        const IoUringPollSubmitResult poll_result =
            try_submit_io_uring_net_poll(channel, channel->interests);
        if (poll_result != IoUringPollSubmitResult::Submitted) {
            static_cast<void>(fallback_io_uring_net_poll_to_epoll(channel));
        }
    }

    if (channel->on_event != nullptr) {
        channel->on_event(channel->owner, events);
    }

    if (!more) {
        destroy_io_uring_operation(operation);
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::fail_io_uring_net_poll(IoUringOperation *operation,
                                                                       int error) noexcept {
    if (operation == nullptr || !operation->net_poll) {
        return false;
    }

    detail::NetIoChannel *channel = operation->net_channel;
    if (channel != nullptr && channel->backend_token == operation) {
        channel->backend_token = nullptr;
        channel->active = false;
        if (!fallback_io_uring_net_poll_to_epoll(channel) && channel->on_event != nullptr) {
            channel->on_event(channel->owner, detail::net_io_error);
        }
    }
    operation->net_channel = nullptr;
    static_cast<void>(error);
    return true;
}
#endif

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::update_epoll_net_channel_interest(detail::NetIoChannel *channel,
                                                                    std::uint32_t events) noexcept {
    if (io_epoll_fd_ < 0 || channel == nullptr || channel->fd < 0) {
        return false;
    }
    if ((events & (detail::net_io_readable | detail::net_io_writable)) == 0U) {
        if (channel->active) {
            static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, channel->fd, nullptr));
        }
        channel->active = false;
        channel->interests = 0;
        channel->backend_token = nullptr;
        return true;
    }

    epoll_event event{};
    event.events = epoll_events_for_net_channel(*channel, events);
    event.data.u64 = epoll_channel_token(channel);
    if (channel->active) {
        if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, channel->fd, &event) != 0) {
            return false;
        }
    } else if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, channel->fd, &event) != 0) {
        if (errno != EEXIST || ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, channel->fd, &event) != 0) {
            return false;
        }
    }
    channel->active = true;
    channel->interests = events;
    return true;
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::update_net_channel_interest(detail::NetIoChannel *channel,
                                                              std::uint32_t events) noexcept {
    if (io_epoll_fd_ < 0 || channel == nullptr || channel->fd < 0) {
        return false;
    }

#if defined(__linux__)
    if (channel->backend_token != nullptr) {
        if ((events & (detail::net_io_readable | detail::net_io_writable)) == 0U) {
            detach_io_uring_net_poll(channel);
            channel->active = false;
            channel->interests = 0;
            return true;
        }
        if (events == channel->interests) {
            return true;
        }
        detach_io_uring_net_poll(channel);
        channel->active = false;
        channel->interests = events;
        const IoUringPollSubmitResult poll_result = try_submit_io_uring_net_poll(channel, events);
        if (poll_result == IoUringPollSubmitResult::Submitted) {
            return true;
        }
        if (poll_result == IoUringPollSubmitResult::Failed) {
            return false;
        }
    } else if (!channel->active &&
               (events & (detail::net_io_readable | detail::net_io_writable)) != 0U) {
        const IoUringPollSubmitResult poll_result = try_submit_io_uring_net_poll(channel, events);
        if (poll_result == IoUringPollSubmitResult::Submitted) {
            return true;
        }
        if (poll_result == IoUringPollSubmitResult::Failed) {
            return false;
        }
    }
#endif

    return update_epoll_net_channel_interest(channel, events);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::update_epoll_interest(int fd, const IoWaitEntry &entry) noexcept {
    const std::uint32_t native_events = epoll_events_for_entry(entry);
    if (native_events == 0U) {
        static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
        return true;
    }

    epoll_event event{};
    event.events = native_events;
    event.data.u64 = epoll_wait_token(fd);
    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) == 0) {
        return true;
    }
    if (errno != ENOENT) {
        return false;
    }
    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0) {
        return true;
    }
    if (errno == EEXIST) {
        return ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) == 0;
    }
    return false;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                                     IoResult *result, bool prefer_rearm) noexcept {
    static_cast<void>(prefer_rearm);
    const bool unsupported_events = (events & (io_readable | io_writable)) == 0U;
    auto existing = io_waits_.find(fd);
    if (io_epoll_fd_ < 0 || fd < 0 || events == 0U || unsupported_events ||
        (existing != io_waits_.end() && io_wait_events_conflict(existing->second, events)) ||
        net_channels_.find(fd) != net_channels_.end()) {
        int error = 0;
        if (fd < 0) {
            error = EBADF;
        } else if (events == 0U || unsupported_events) {
            error = EINVAL;
        } else if (io_epoll_fd_ < 0) {
            error = ENOSYS;
        } else {
            error = EALREADY;
        }
        detail::set_io_result_error(*result, fd, error);
        return false;
    }

    IoWaitRegistration *registration = nullptr;
    typename absl::flat_hash_map<int, IoWaitEntry>::iterator wait_it;
    try {
        registration = io_wait_pool_.create();
        registration->fd = fd;
        registration->events = events;
        registration->task = task;
        registration->result = result;
        registration->poll_operation = nullptr;
        auto [it, inserted] = io_waits_.try_emplace(fd);
        static_cast<void>(inserted);
        wait_it = it;
        add_io_wait_registration(wait_it->second, registration);
    } catch (...) {
        if (registration != nullptr) {
            io_wait_pool_.destroy(registration);
        }
        detail::set_io_result_error(*result, fd, ENOMEM);
        return false;
    }

    const IoUringPollSubmitResult poll_result =
        try_submit_io_uring_poll_wait(fd, events, task, result, registration);
    if (poll_result == IoUringPollSubmitResult::Submitted) {
        *result = IoResult{fd, 0, 0};
        return true;
    }
    if (poll_result == IoUringPollSubmitResult::Failed) {
        remove_io_wait_registration(wait_it->second, registration);
        if (io_wait_entry_empty(wait_it->second)) {
            io_waits_.erase(wait_it);
        }
        io_wait_pool_.destroy(registration);
        return false;
    }
    if (poll_result == IoUringPollSubmitResult::BackendClosed) {
        registration->poll_operation = nullptr;
        *result = IoResult{fd, 0, 0};
    }

    if (!update_epoll_interest(fd, wait_it->second)) {
        const int error = errno == 0 ? EIO : errno;
        remove_io_wait_registration(wait_it->second, registration);
        if (io_wait_entry_empty(wait_it->second)) {
            io_waits_.erase(wait_it);
            static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
        } else {
            static_cast<void>(update_epoll_interest(fd, wait_it->second));
        }
        io_wait_pool_.destroy(registration);
        detail::set_io_result_error(*result, fd, error);
        return false;
    }

    *result = IoResult{fd, 0, 0};
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::cancel_native_io_wait(IoOpState &state) noexcept {
    if (io_epoll_fd_ < 0) {
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

    if (registration->poll_operation != nullptr) {
        IoUringOperation *operation = registration->poll_operation;
        const int submit_error = submit_io_uring_cancel(operation);
        if (submit_error != 0) {
            state.wait.fd = fd;
            state.wait.events = io_error;
            state.wait.error = submit_error;
            state.wait.result = -submit_error;
            return false;
        }

        remove_io_wait_registration(it->second, registration);
        if (io_wait_entry_empty(it->second)) {
            io_waits_.erase(it);
        }
        registration->poll_operation = nullptr;
        if (operation->wait_registration == registration) {
            operation->wait_registration = nullptr;
        }
        operation->cancel_requested = true;
        operation->task = nullptr;
        operation->result = nullptr;

        detail::set_io_result_error(state.wait, fd, ECANCELED);
        if (registration->task != running_task_) {
            enqueue_pending_blocking(index_, registration->task);
        }
        io_wait_pool_.destroy(registration);
        return true;
    }
    remove_io_wait_registration(it->second, registration);
    if (io_wait_entry_empty(it->second)) {
        io_waits_.erase(it);
        static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
    } else {
        static_cast<void>(update_epoll_interest(fd, it->second));
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
