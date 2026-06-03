#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_epoll_backend.hpp is internal"
#endif

namespace af::detail {

#if AF_DETAIL_HAS_EPOLL
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
    std::uint32_t result = POLLERR | POLLHUP;
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
    event.data.fd = -1;
    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
        close_native_io_backend();
        return false;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_native_io_backend() noexcept {
    clear_io_waits();
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
    if (timeout_ms == 0 && io_waits_.empty()) {
        return did_work;
    }

    std::array<epoll_event, 64> events;
    const int count =
        ::epoll_wait(io_epoll_fd_, events.data(), static_cast<int>(events.size()), timeout_ms);
    if (count <= 0) {
        return did_work;
    }

    for (int i = 0; i < count; ++i) {
        const int fd = events[static_cast<std::size_t>(i)].data.fd;
        if (fd < 0) {
            drain_io_wake();
            if (poll_io_uring_completions()) {
                did_work = true;
            }
            did_work = true;
            continue;
        }

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
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::update_epoll_interest(int fd, const IoWaitEntry &entry) noexcept {
    const std::uint32_t native_events = epoll_events_for_entry(entry);
    if (native_events == 0U) {
        static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
        return true;
    }

    epoll_event event{};
    event.events = native_events;
    event.data.fd = fd;
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
        (existing != io_waits_.end() && io_wait_events_conflict(existing->second, events))) {
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
