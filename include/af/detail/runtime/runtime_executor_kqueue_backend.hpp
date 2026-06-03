#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_kqueue_backend.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

#if AF_DETAIL_HAS_KQUEUE
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::native_io_backend_available() const noexcept {
    return io_kqueue_fd_ >= 0;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::notify_native_io_backend() noexcept {
    if (io_kqueue_fd_ < 0) {
        return false;
    }
    bool expected = false;
    if (!io_wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
        return true;
    }

    struct kevent event{};
    EV_SET(&event, kqueue_wake_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
        io_wake_pending_.store(false, std::memory_order_relaxed);
        return false;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::init_native_io_backend() noexcept {
    if (!native_io_thread()) {
        return false;
    }
    if (io_kqueue_fd_ >= 0) {
        return true;
    }
    reserve_native_io_wait_storage();

    io_kqueue_fd_ = ::kqueue();
    if (io_kqueue_fd_ < 0) {
        return false;
    }

    struct kevent event{};
    EV_SET(&event, kqueue_wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
        close_native_io_backend();
        return false;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_native_io_backend() noexcept {
    clear_io_waits();
    clear_kqueue_timeouts();
    for (auto &entry : net_channels_) {
        if (entry.second != nullptr) {
            entry.second->active = false;
            entry.second->interests = 0;
        }
    }
    net_channels_.clear();
    if (io_kqueue_fd_ >= 0) {
        ::close(io_kqueue_fd_);
        io_kqueue_fd_ = -1;
    }
    io_wake_pending_.store(false, std::memory_order_relaxed);
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] intptr_t
Executor<RuntimeT, TraitsT>::kqueue_timeout_data(std::chrono::nanoseconds timeout) noexcept {
#if defined(NOTE_NSECONDS)
    return clamp_kqueue_timer_value(timeout.count());
#elif defined(NOTE_USECONDS)
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        timeout + std::chrono::nanoseconds{999});
    return clamp_kqueue_timer_value(us.count());
#else
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        timeout + std::chrono::nanoseconds{999999});
    return clamp_kqueue_timer_value(ms.count() == 0 ? 1 : ms.count());
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t Executor<RuntimeT, TraitsT>::kqueue_timeout_unit_flags() noexcept {
#if defined(NOTE_NSECONDS)
    return NOTE_NSECONDS;
#elif defined(NOTE_USECONDS)
    return NOTE_USECONDS;
#else
    return 0;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] intptr_t
Executor<RuntimeT, TraitsT>::clamp_kqueue_timer_value(std::int64_t value) noexcept {
    constexpr auto max_value = static_cast<std::uint64_t>(std::numeric_limits<intptr_t>::max());
    if (value <= 0) {
        return 1;
    }
    const auto unsigned_value = static_cast<std::uint64_t>(value);
    if (unsigned_value > max_value) {
        return std::numeric_limits<intptr_t>::max();
    }
    return static_cast<intptr_t>(value);
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_kqueue_timeouts() noexcept {
    KqueueTimeoutRegistration *registration = io_kqueue_timeouts_;
    while (registration != nullptr) {
        KqueueTimeoutRegistration *next = registration->next;
        if (registration->result != nullptr &&
            registration->result->completion_token == registration) {
            registration->result->completion_token = nullptr;
        }
        io_kqueue_timeout_pool_.destroy(registration);
        registration = next;
    }
    io_kqueue_timeouts_ = nullptr;
    io_kqueue_timeout_count_ = 0;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::track_kqueue_timeout(
    KqueueTimeoutRegistration *registration) noexcept {
    registration->prev = nullptr;
    registration->next = io_kqueue_timeouts_;
    if (io_kqueue_timeouts_ != nullptr) {
        io_kqueue_timeouts_->prev = registration;
    }
    io_kqueue_timeouts_ = registration;
    ++io_kqueue_timeout_count_;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::untrack_kqueue_timeout(
    KqueueTimeoutRegistration *registration) noexcept {
    if (registration->prev != nullptr) {
        registration->prev->next = registration->next;
    } else if (io_kqueue_timeouts_ == registration) {
        io_kqueue_timeouts_ = registration->next;
    }
    if (registration->next != nullptr) {
        registration->next->prev = registration->prev;
    }
    registration->prev = nullptr;
    registration->next = nullptr;
    if (io_kqueue_timeout_count_ != 0U) {
        --io_kqueue_timeout_count_;
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] uintptr_t Executor<RuntimeT, TraitsT>::next_kqueue_timeout_ident() noexcept {
    uintptr_t ident = io_kqueue_next_timeout_ident_++;
    if (ident <= kqueue_wake_ident) {
        ident = kqueue_wake_ident + 1U;
        io_kqueue_next_timeout_ident_ = ident + 1U;
    }
    return ident;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::submit_kqueue_timeout(std::chrono::nanoseconds timeout, Task *task,
                                                   IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "kqueue timeout submit must run on its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr ||
        timeout.count() <= 0) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, -1, EINVAL);
        }
        return false;
    }
    if (io_kqueue_fd_ < 0) {
        detail::set_io_result_error(*result, -1, ENOSYS);
        return false;
    }

    KqueueTimeoutRegistration *registration = nullptr;
    try {
        registration = io_kqueue_timeout_pool_.create();
    } catch (...) {
        detail::set_io_result_error(*result, -1, ENOMEM);
        return false;
    }

    registration->task = task;
    registration->result = result;
    registration->prev = nullptr;
    registration->next = nullptr;
    registration->ident = next_kqueue_timeout_ident();

    struct kevent event{};
    EV_SET(&event, registration->ident, EVFILT_TIMER, EV_ADD | EV_ONESHOT,
           kqueue_timeout_unit_flags(), kqueue_timeout_data(timeout), registration);
    if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
        const int error = errno == 0 ? EIO : errno;
        io_kqueue_timeout_pool_.destroy(registration);
        detail::set_io_result_error(*result, -1, error);
        return false;
    }

    track_kqueue_timeout(registration);
    result->fd = -1;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = registration;
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::cancel_kqueue_timeout(IoOpState &state) noexcept {
    if (io_kqueue_fd_ < 0) {
        detail::set_io_result_error(state.wait, state.wait.fd, ENOSYS);
        return false;
    }

    auto *registration = static_cast<KqueueTimeoutRegistration *>(state.wait.completion_token);
    if (registration == nullptr || registration->result != &state.wait) {
        detail::set_io_result_error(state.wait, state.wait.fd, ENOENT);
        return false;
    }

    struct kevent event{};
    EV_SET(&event, registration->ident, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
    if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0 && errno != ENOENT) {
        const int error = errno == 0 ? EIO : errno;
        state.wait.events = io_error;
        state.wait.error = error;
        state.wait.result = -error;
        return false;
    }

    untrack_kqueue_timeout(registration);
    detail::set_io_result_error(state.wait, -1, ECANCELED);
    if (registration->task != running_task_) {
        enqueue_pending_blocking(index_, registration->task);
    }
    io_kqueue_timeout_pool_.destroy(registration);
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::complete_kqueue_timeout(KqueueTimeoutRegistration *registration,
                                                     const struct kevent &event) noexcept {
    if (registration == nullptr || registration->result == nullptr) {
        return false;
    }

    IoResult *result = registration->result;
    if (result->completion_token != registration) {
        return false;
    }

    untrack_kqueue_timeout(registration);
    const int error = io_error_from_kqueue(event);
    detail::set_io_result_error(*result, -1, error == 0 ? ETIMEDOUT : error);
    enqueue_pending_blocking(index_, registration->task);
    io_kqueue_timeout_pool_.destroy(registration);
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_native_io(int timeout_ms,
                                                               bool did_work) noexcept {
    if (io_kqueue_fd_ < 0) {
        return did_work;
    }
    if (timeout_ms == 0 && io_waits_.empty() && net_channels_.empty() &&
        io_kqueue_timeout_count_ == 0U && !io_wake_pending_.load(std::memory_order_acquire)) {
        return did_work;
    }

    timespec timeout{};
    timespec *timeout_ptr = nullptr;
    if (timeout_ms >= 0) {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
        timeout_ptr = &timeout;
    }

    std::array<struct kevent, 64> events;
    const int count = ::kevent(io_kqueue_fd_, nullptr, 0, events.data(),
                               static_cast<int>(events.size()), timeout_ptr);
    if (count <= 0) {
        return did_work;
    }

    std::array<IoWaitRegistration *, 64> completed{};
    std::size_t completed_count = 0;
    for (int i = 0; i < count; ++i) {
        const struct kevent &event = events[static_cast<std::size_t>(i)];
        if (event.filter == EVFILT_USER) {
            io_wake_pending_.store(false, std::memory_order_relaxed);
            did_work = true;
            continue;
        }
        if (event.filter == EVFILT_TIMER) {
            auto *timeout = static_cast<KqueueTimeoutRegistration *>(event.udata);
            if (complete_kqueue_timeout(timeout, event)) {
                did_work = true;
            }
            continue;
        }

        const int fd = static_cast<int>(event.ident);
        auto net_it = net_channels_.find(fd);
        if (net_it != net_channels_.end()) {
            detail::NetIoChannel *channel = net_it->second;
            if (channel != nullptr && channel->active && channel->on_event != nullptr) {
                channel->on_event(channel->owner, net_events_from_kqueue(event));
                did_work = true;
            }
            continue;
        }

        auto *registration = static_cast<IoWaitRegistration *>(event.udata);
        if (registration == nullptr) {
            continue;
        }

        const int wait_fd = registration->fd;
        auto it = io_waits_.find(wait_fd);
        if (it == io_waits_.end() || !io_wait_entry_contains(it->second, registration)) {
            continue;
        }

        remove_kqueue_filters(*registration);
        remove_io_wait_registration(it->second, registration);
        if (io_wait_entry_empty(it->second)) {
            io_waits_.erase(it);
        }

        const int error = io_error_from_kqueue(event);
        if (error != 0) {
            detail::set_io_result_error(*registration->result, wait_fd, error);
        } else {
            registration->result->fd = wait_fd;
            registration->result->events = io_events_from_kqueue(event);
            registration->result->error = 0;
        }
        enqueue_pending_blocking(index_, registration->task);
        completed[completed_count++] = registration;
        did_work = true;
    }

    for (std::size_t i = 0; i < completed_count; ++i) {
        io_wait_pool_.destroy(completed[i]);
    }
    return did_work;
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::update_net_channel_interest(detail::NetIoChannel *channel,
                                                              std::uint32_t events) noexcept {
    if (io_kqueue_fd_ < 0 || channel == nullptr || channel->fd < 0) {
        return false;
    }

    const std::uint32_t old_events = channel->interests;
    std::array<struct kevent, 2> changes;
    int count = 0;
    auto update_filter = [&](std::uint32_t bit, int16_t filter) {
        const bool had = (old_events & bit) != 0U;
        const bool wants = (events & bit) != 0U;
        if (had == wants) {
            return;
        }
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(channel->fd),
               filter, wants ? EV_ADD : EV_DELETE, 0, 0, channel);
    };
    update_filter(detail::net_io_readable, EVFILT_READ);
    update_filter(detail::net_io_writable, EVFILT_WRITE);

    if (count != 0 && ::kevent(io_kqueue_fd_, changes.data(), count, nullptr, 0, nullptr) != 0) {
        return false;
    }

    channel->interests = events;
    channel->active = events != 0U;
    return true;
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
            io_kqueue_timeout_pool_.reserve_slots(io_wait_reserve);
        }
    } catch (...) {
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                                     IoResult *result, bool prefer_rearm) noexcept {
    static_cast<void>(prefer_rearm);
    const bool unsupported_events = (events & (io_readable | io_writable)) == 0U;
    auto existing = io_waits_.find(fd);
    if (io_kqueue_fd_ < 0 || fd < 0 || events == 0U || unsupported_events ||
        net_channels_.find(fd) != net_channels_.end() ||
        (existing != io_waits_.end() && io_wait_events_conflict(existing->second, events))) {
        int error = 0;
        if (fd < 0) {
            error = EBADF;
        } else if (events == 0U || unsupported_events) {
            error = EINVAL;
        } else if (io_kqueue_fd_ < 0) {
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

    std::array<struct kevent, 2> changes;
    int change_count = fill_kqueue_changes(fd, events, registration, changes);
    if (::kevent(io_kqueue_fd_, changes.data(), change_count, nullptr, 0, nullptr) != 0) {
        const int error = errno == 0 ? EIO : errno;
        remove_io_wait_registration(wait_it->second, registration);
        if (io_wait_entry_empty(wait_it->second)) {
            io_waits_.erase(wait_it);
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
    if (io_kqueue_fd_ < 0) {
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

    remove_kqueue_filters(*registration);
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

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int
Executor<RuntimeT, TraitsT>::fill_kqueue_changes(int fd, std::uint32_t events,
                                                 IoWaitRegistration *registration,
                                                 std::array<struct kevent, 2> &changes) noexcept {
    int count = 0;
    if ((events & io_readable) != 0U) {
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(fd), EVFILT_READ,
               EV_ADD | EV_ONESHOT, 0, 0, registration);
    }
    if ((events & io_writable) != 0U) {
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(fd),
               EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, registration);
    }
    return count;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::remove_kqueue_filters(
    const IoWaitRegistration &registration) noexcept {
    std::array<struct kevent, 2> changes;
    int count = 0;
    if ((registration.events & io_readable) != 0U) {
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(registration.fd),
               EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    }
    if ((registration.events & io_writable) != 0U) {
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(registration.fd),
               EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    }
    if (count != 0) {
        static_cast<void>(::kevent(io_kqueue_fd_, changes.data(), count, nullptr, 0, nullptr));
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::io_events_from_kqueue(const struct kevent &event) noexcept {
    if ((event.flags & EV_ERROR) != 0) {
        return io_error;
    }

    std::uint32_t result = 0;
    if (event.filter == EVFILT_READ) {
        result |= io_readable;
    } else if (event.filter == EVFILT_WRITE) {
        result |= io_writable;
    }
    if ((event.flags & EV_EOF) != 0) {
        result |= io_hangup;
    }
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] std::uint32_t
Executor<RuntimeT, TraitsT>::net_events_from_kqueue(const struct kevent &event) noexcept {
    if ((event.flags & EV_ERROR) != 0) {
        return detail::net_io_error;
    }

    std::uint32_t result = 0;
    if (event.filter == EVFILT_READ) {
        result |= detail::net_io_readable;
    } else if (event.filter == EVFILT_WRITE) {
        result |= detail::net_io_writable;
    }
    if ((event.flags & EV_EOF) != 0) {
        result |= detail::net_io_hangup;
    }
    return result;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int
Executor<RuntimeT, TraitsT>::io_error_from_kqueue(const struct kevent &event) noexcept {
    if ((event.flags & EV_ERROR) == 0) {
        return 0;
    }
    return event.data == 0 ? EIO : static_cast<int>(event.data);
}
#endif

} // namespace af::detail
