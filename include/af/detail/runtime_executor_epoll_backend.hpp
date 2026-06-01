#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_epoll_backend.hpp is internal"
#endif

namespace af::detail {

#if AF_DETAIL_HAS_EPOLL
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::native_io_backend_available() const noexcept {
  return io_epoll_fd_ >= 0;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::notify_native_io_backend() noexcept {
  if (io_epoll_fd_ < 0) {
    return false;
  }
  bool expected = false;
  if (io_wake_pending_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
    const std::uint64_t value = 1;
    const auto written = ::write(io_wake_fd_, &value, sizeof(value));
    static_cast<void>(written);
  }
  return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::init_native_io_backend() noexcept {
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
  event.data.ptr = nullptr;
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
    io_wait_pool_.destroy(entry.second);
  }
  io_waits_.clear();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::poll_native_io(int timeout_ms,
                                            bool did_work) noexcept {
  if (io_epoll_fd_ < 0) {
    return did_work;
  }
  if (timeout_ms == 0 && io_waits_.empty()) {
    return did_work;
  }

  std::array<epoll_event, 64> events;
  const int count = ::epoll_wait(io_epoll_fd_, events.data(),
                                 static_cast<int>(events.size()), timeout_ms);
  if (count <= 0) {
    return did_work;
  }

  for (int i = 0; i < count; ++i) {
    auto *registration = static_cast<IoWaitRegistration *>(
        events[static_cast<std::size_t>(i)].data.ptr);
    if (registration == nullptr) {
      drain_io_wake();
      if (poll_io_uring_completions()) {
        did_work = true;
      }
      did_work = true;
      continue;
    }

    const int fd = registration->fd;
    io_waits_.erase(fd);
    static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));

    registration->result->fd = fd;
    registration->result->events =
        io_events_from_native(events[static_cast<std::size_t>(i)].events);
    registration->result->error = 0;
    enqueue_pending_blocking(index_, registration->task);
    io_wait_pool_.destroy(registration);
    did_work = true;
  }
  return did_work;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::register_native_io_wait(
    int fd, std::uint32_t events, Task *task, IoResult *result,
    bool prefer_rearm) noexcept {
  static_cast<void>(prefer_rearm);
  if (io_epoll_fd_ < 0 || fd < 0 || events == 0U ||
      io_waits_.find(fd) != io_waits_.end()) {
    result->fd = fd;
    result->events = io_error;
    if (fd < 0) {
      result->error = EBADF;
    } else if (events == 0U) {
      result->error = EINVAL;
    } else if (io_epoll_fd_ < 0) {
      result->error = ENOSYS;
    } else {
      result->error = EALREADY;
    }
    return false;
  }

  IoWaitRegistration *registration = nullptr;
  try {
    registration = io_wait_pool_.create();
    auto [it, inserted] = io_waits_.emplace(fd, registration);
    static_cast<void>(it);
    if (!inserted) {
      io_wait_pool_.destroy(registration);
      result->fd = fd;
      result->events = io_error;
      result->error = EALREADY;
      return false;
    }
  } catch (...) {
    if (registration != nullptr) {
      io_wait_pool_.destroy(registration);
    }
    result->fd = fd;
    result->events = io_error;
    result->error = ENOMEM;
    return false;
  }
  registration->fd = fd;
  registration->events = events;
  registration->task = task;
  registration->result = result;
  registration->poll_operation = nullptr;

  const IoUringPollSubmitResult poll_result =
      try_submit_io_uring_poll_wait(fd, events, task, result, registration);
  if (poll_result == IoUringPollSubmitResult::Submitted) {
    *result = IoResult{fd, 0, 0};
    return true;
  }
  if (poll_result == IoUringPollSubmitResult::Failed) {
    io_waits_.erase(fd);
    io_wait_pool_.destroy(registration);
    return false;
  }
  if (poll_result == IoUringPollSubmitResult::BackendClosed) {
    return false;
  }

  std::uint32_t native_events = EPOLLERR | EPOLLHUP | EPOLLONESHOT;
  if ((events & io_readable) != 0U) {
    native_events |= EPOLLIN;
  }
  if ((events & io_writable) != 0U) {
    native_events |= EPOLLOUT;
  }

  epoll_event event{};
  event.events = native_events;
  event.data.ptr = registration;

  if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
    const int first_error = errno;
    if (first_error != EEXIST ||
        ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) != 0) {
      io_waits_.erase(fd);
      io_wait_pool_.destroy(registration);
      result->fd = fd;
      result->events = io_error;
      result->error = errno;
      return false;
    }
  }

  *result = IoResult{fd, 0, 0};
  return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::cancel_native_io_wait(IoOpState &state) noexcept {
  if (io_epoll_fd_ < 0) {
    state.wait.events = io_error;
    state.wait.error = ENOSYS;
    state.wait.result = -ENOSYS;
    return false;
  }

  const int fd = state.wait.fd;
  auto it = io_waits_.find(fd);
  if (fd < 0 || it == io_waits_.end() || it->second->result != &state.wait) {
    state.wait.events = io_error;
    state.wait.error = ENOENT;
    state.wait.result = -ENOENT;
    return false;
  }

  IoWaitRegistration *registration = it->second;
  if (registration->poll_operation != nullptr) {
    IoUringOperation *operation = registration->poll_operation;
    const int submit_error = submit_io_uring_cancel(operation);
    if (submit_error != 0) {
      state.wait.events = io_error;
      state.wait.error = submit_error;
      state.wait.result = -submit_error;
      return false;
    }

    io_waits_.erase(it);
    registration->poll_operation = nullptr;
    if (operation->wait_registration == registration) {
      operation->wait_registration = nullptr;
    }
    operation->cancel_requested = true;
    operation->task = nullptr;
    operation->result = nullptr;

    state.wait.fd = fd;
    state.wait.events = io_error;
    state.wait.error = ECANCELED;
    state.wait.result = -ECANCELED;
    if (registration->task != running_task_) {
      enqueue_pending_blocking(index_, registration->task);
    }
    io_wait_pool_.destroy(registration);
    return true;
  }
  io_waits_.erase(it);
  static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));

  state.wait.fd = fd;
  state.wait.events = io_error;
  state.wait.error = ECANCELED;
  state.wait.result = -ECANCELED;
  if (registration->task != running_task_) {
    enqueue_pending_blocking(index_, registration->task);
  }
  io_wait_pool_.destroy(registration);
  return true;
}
#endif

} // namespace af::detail
