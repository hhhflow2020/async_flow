#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_io_backend.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::init_io_backend() noexcept {
  if (!io_thread() || native_io_backend_available()) {
    return;
  }
  if (!init_native_io_backend()) {
#if defined(__linux__)
    if (io_uring_thread()) {
      io_uring_backend_error_ = ENODEV;
    }
#endif
    return;
  }
#if defined(__linux__)
  if (io_uring_thread()) {
    init_io_uring_backend();
  }
#endif
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_io_backend() noexcept {
#if defined(__linux__)
  close_io_uring_backend();
#endif
  close_native_io_backend();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
  bool did_work = poll_io_uring_completions();
#else
  bool did_work = false;
#endif
  return poll_native_io(timeout_ms, did_work);
}

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::detect_io_uring_features() noexcept {
  io_uring_send_zc_available_ = false;
  io_uring_sendmsg_zc_available_ = false;
  io_uring_poll_add_available_ = false;
  io_uring_socket_available_ = false;

  constexpr unsigned probe_count = 64;
  std::array<std::byte,
             sizeof(io_uring_probe) + probe_count * sizeof(io_uring_probe_op)>
      storage{};
  auto *probe = reinterpret_cast<io_uring_probe *>(storage.data());
  if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_PROBE, probe,
                                    probe_count) != 0) {
    return;
  }

  const auto *ops = reinterpret_cast<const io_uring_probe_op *>(
      storage.data() + sizeof(io_uring_probe));
  const unsigned op_count = std::min<unsigned>(probe->ops_len, probe_count);
  for (unsigned i = 0; i < op_count; ++i) {
    if (ops[i].op == detail::io_uring_op_send_zc &&
        (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
      io_uring_send_zc_available_ = true;
    } else if (ops[i].op == detail::io_uring_op_sendmsg_zc &&
               (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
      io_uring_sendmsg_zc_available_ = true;
    } else if (ops[i].op == IORING_OP_POLL_ADD &&
               (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
      io_uring_poll_add_available_ = true;
    } else if (ops[i].op == detail::io_uring_op_socket &&
               (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
      io_uring_socket_available_ = true;
    }
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_io_uring_backend() noexcept {
  clear_io_uring_operations();
  if (io_uring_fd_ >= 0 && io_uring_files_registered_) {
    static_cast<void>(detail::sys_io_uring_register(
        io_uring_fd_, IORING_UNREGISTER_FILES, nullptr, 0));
  }
  if (io_uring_fd_ >= 0 && io_uring_buffers_registered_) {
    static_cast<void>(detail::sys_io_uring_register(
        io_uring_fd_, IORING_UNREGISTER_BUFFERS, nullptr, 0));
  }
  if (io_uring_sqes_ != nullptr && io_uring_sqes_ != MAP_FAILED) {
    ::munmap(io_uring_sqes_, io_uring_sqes_size_);
  }
  if (io_uring_sq_ring_ != nullptr && io_uring_sq_ring_ != MAP_FAILED) {
    ::munmap(io_uring_sq_ring_, io_uring_sq_ring_size_);
  }
  if (io_uring_cq_ring_ != nullptr && io_uring_cq_ring_ != MAP_FAILED &&
      io_uring_cq_ring_ != io_uring_sq_ring_) {
    ::munmap(io_uring_cq_ring_, io_uring_cq_ring_size_);
  }
  if (io_uring_fd_ >= 0) {
    ::close(io_uring_fd_);
  }

  io_uring_fd_ = -1;
  io_uring_backend_error_ = 0;
  io_uring_sq_ring_ = nullptr;
  io_uring_cq_ring_ = nullptr;
  io_uring_sqes_ = nullptr;
  io_uring_sq_ring_size_ = 0;
  io_uring_cq_ring_size_ = 0;
  io_uring_sqes_size_ = 0;
  io_uring_sq_head_ = nullptr;
  io_uring_sq_tail_ = nullptr;
  io_uring_sq_ring_mask_ = nullptr;
  io_uring_sq_ring_entries_ = nullptr;
  io_uring_sq_array_ = nullptr;
  io_uring_cq_head_ = nullptr;
  io_uring_cq_tail_ = nullptr;
  io_uring_cq_ring_mask_ = nullptr;
  io_uring_cqes_ = nullptr;
  io_uring_pending_submissions_ = 0;
  io_uring_send_zc_available_ = false;
  io_uring_sendmsg_zc_available_ = false;
  io_uring_poll_add_available_ = false;
  io_uring_socket_available_ = false;
  io_uring_buffers_registered_ = false;
  io_uring_registered_buffer_count_ = 0;
  io_uring_provided_buffer_groups_.clear();
  io_uring_files_registered_ = false;
  io_uring_registered_file_count_ = 0;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
template <typename T>
[[nodiscard]] T *
Executor<RuntimeT, TraitsT>::ptr_at(std::byte *base,
                                    std::uint32_t offset) noexcept {
  return reinterpret_cast<T *>(base + offset);
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] unsigned
Executor<RuntimeT, TraitsT>::io_uring_requested_setup_flags() noexcept {
  unsigned requested_setup_flags = io_uring_setup_flags;
  if constexpr (io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) {
    requested_setup_flags |= IORING_SETUP_SQPOLL;
  }
  if constexpr (io_uring_setup_submit_all) {
    requested_setup_flags |= IORING_SETUP_SUBMIT_ALL;
  }
  if constexpr (io_uring_setup_coop_taskrun) {
    requested_setup_flags |= IORING_SETUP_COOP_TASKRUN;
  }
  if constexpr (io_uring_setup_single_issuer || io_uring_setup_defer_taskrun) {
    requested_setup_flags |= IORING_SETUP_SINGLE_ISSUER;
  }
  if constexpr (io_uring_setup_defer_taskrun) {
    requested_setup_flags |= IORING_SETUP_DEFER_TASKRUN;
  }
  return requested_setup_flags;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::map_io_uring_rings(
    const io_uring_params &params) noexcept {
  const std::size_t sq_ring_size =
      params.sq_off.array +
      static_cast<std::size_t>(params.sq_entries) * sizeof(std::uint32_t);
  const std::size_t cq_ring_size =
      params.cq_off.cqes +
      static_cast<std::size_t>(params.cq_entries) * sizeof(io_uring_cqe);

  if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
    io_uring_sq_ring_size_ = std::max(sq_ring_size, cq_ring_size);
    io_uring_cq_ring_size_ = io_uring_sq_ring_size_;
    io_uring_sq_ring_ = static_cast<std::byte *>(
        ::mmap(nullptr, io_uring_sq_ring_size_, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQ_RING));
    io_uring_cq_ring_ = io_uring_sq_ring_;
  } else {
    io_uring_sq_ring_size_ = sq_ring_size;
    io_uring_cq_ring_size_ = cq_ring_size;
    io_uring_sq_ring_ = static_cast<std::byte *>(
        ::mmap(nullptr, io_uring_sq_ring_size_, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQ_RING));
    io_uring_cq_ring_ = static_cast<std::byte *>(
        ::mmap(nullptr, io_uring_cq_ring_size_, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_CQ_RING));
  }

  io_uring_sqes_size_ =
      static_cast<std::size_t>(params.sq_entries) * sizeof(io_uring_sqe);
  io_uring_sqes_ = static_cast<io_uring_sqe *>(
      ::mmap(nullptr, io_uring_sqes_size_, PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQES));

  return io_uring_sq_ring_ != MAP_FAILED && io_uring_cq_ring_ != MAP_FAILED &&
         io_uring_sqes_ != MAP_FAILED;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::bind_io_uring_ring_pointers(
    const io_uring_params &params) noexcept {
  io_uring_sq_head_ =
      ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.head);
  io_uring_sq_tail_ =
      ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.tail);
  io_uring_sq_ring_mask_ =
      ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_mask);
  io_uring_sq_ring_entries_ =
      ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_entries);
  io_uring_sq_array_ =
      ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.array);
  io_uring_cq_head_ =
      ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.head);
  io_uring_cq_tail_ =
      ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.tail);
  io_uring_cq_ring_mask_ =
      ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.ring_mask);
  io_uring_cqes_ = ptr_at<io_uring_cqe>(io_uring_cq_ring_, params.cq_off.cqes);
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::register_io_uring_wake_fd() noexcept {
  return detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_EVENTFD,
                                       &io_wake_fd_, 1) == 0;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::init_io_uring_backend() noexcept {
  if (io_uring_fd_ >= 0) {
    return;
  }
  if (io_wake_fd_ < 0) {
    io_uring_backend_error_ = ENODEV;
    return;
  }

  io_uring_backend_error_ = 0;
  io_uring_params params{};
  detail::configure_io_uring_params(
      params, detail::IoUringSetupRequest{
                  io_uring_requested_setup_flags(), io_uring_cq_entries,
                  io_uring_sqpoll_idle_ms, io_uring_sqpoll_cpu});
  io_uring_fd_ = detail::sys_io_uring_setup(io_uring_entries, &params);
  if (io_uring_fd_ < 0) {
    io_uring_backend_error_ = errno == 0 ? EIO : errno;
    return;
  }

  if (!map_io_uring_rings(params)) {
    const int map_error = errno == 0 ? EIO : errno;
    close_io_uring_backend();
    io_uring_backend_error_ = map_error;
    return;
  }

  bind_io_uring_ring_pointers(params);
  detect_io_uring_features();

  if (!register_io_uring_wake_fd()) {
    const int register_error = errno == 0 ? EIO : errno;
    close_io_uring_backend();
    io_uring_backend_error_ = register_error;
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::reserve_io_backend_storage() noexcept {
  try {
    if constexpr (io_wait_reserve != 0U) {
      io_waits_.reserve(io_wait_reserve);
      io_wait_pool_.reserve_slots(io_wait_reserve);
    }
    if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
      io_uring_provided_buffer_groups_.reserve(
          io_uring_provided_buffer_group_reserve);
    }
    if (io_uring_thread()) {
      io_uring_msg_pool_.reserve_slots(io_uring_entries);
      io_uring_address_pool_.reserve_slots(io_uring_entries);
      io_uring_op_pool_.reserve_slots(io_uring_entries);
    }
  } catch (...) {
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] io_uring_sqe *
Executor<RuntimeT, TraitsT>::reserve_io_uring_sqe(int &error) noexcept {
  error = 0;
  if (io_uring_fd_ < 0 || io_uring_sq_tail_ == nullptr ||
      io_uring_sq_head_ == nullptr) {
    error = ENOSYS;
    return nullptr;
  }

  std::uint32_t head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
  std::uint32_t tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
  if (tail - head >= *io_uring_sq_ring_entries_ &&
      io_uring_pending_submissions_ != 0U) {
    const int submit_error = flush_io_uring_submissions();
    if (submit_error != 0) {
      error = submit_error;
      fail_io_uring_backend(submit_error, nullptr);
      return nullptr;
    }
    head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
    tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
  }
  if (tail - head >= *io_uring_sq_ring_entries_) {
    error = EBUSY;
    return nullptr;
  }

  const std::uint32_t index = tail & *io_uring_sq_ring_mask_;
  io_uring_sq_array_[index] = index;
  __atomic_store_n(io_uring_sq_tail_, tail + 1U, __ATOMIC_RELEASE);
  ++io_uring_pending_submissions_;
  return &io_uring_sqes_[index];
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int
Executor<RuntimeT, TraitsT>::flush_io_uring_submissions() noexcept {
  if (io_uring_pending_submissions_ == 0U) {
    return 0;
  }

  unsigned remaining = io_uring_pending_submissions_;
  while (remaining != 0U) {
    const int submitted =
        detail::sys_io_uring_enter(io_uring_fd_, remaining, 0, 0);
    if (submitted > 0) {
      const auto submitted_count = static_cast<unsigned>(submitted);
      if (submitted_count > remaining) {
        return EIO;
      }
      remaining -= submitted_count;
      continue;
    }
    if (submitted == 0) {
      return EIO;
    }
    if (errno == EINTR) {
      continue;
    }
    return errno == 0 ? EIO : errno;
  }

  io_uring_pending_submissions_ = 0;
  return 0;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::flush_io_uring_submissions_or_fail() noexcept {
  const int submit_error = flush_io_uring_submissions();
  if (submit_error == 0) {
    return false;
  }
  fail_io_uring_backend(submit_error, nullptr);
  return true;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::poll_io_uring_completions() noexcept {
  if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr ||
      io_uring_cq_tail_ == nullptr) {
    return false;
  }

  bool did_work = false;
  std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
  const std::uint32_t tail =
      __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
  while (head != tail) {
    io_uring_cqe &cqe = io_uring_cqes_[head & *io_uring_cq_ring_mask_];
    auto *operation = reinterpret_cast<IoUringOperation *>(cqe.user_data);
    if (operation != nullptr) {
      const bool yield_to_task =
          complete_io_uring_operation(operation, cqe.res, cqe.flags);
      did_work = true;
      ++head;
      if (yield_to_task) {
        break;
      }
      continue;
    }
    ++head;
  }
  __atomic_store_n(io_uring_cq_head_, head, __ATOMIC_RELEASE);
  return did_work;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::complete_io_uring_operation(
    IoUringOperation *operation, int result, std::uint32_t cqe_flags) noexcept {
  if (operation->poll_wait) {
    complete_io_uring_poll_wait(operation, result);
    return false;
  }

  if (operation->zero_copy_send && (cqe_flags & IORING_CQE_F_NOTIF) != 0U) {
    operation->zero_copy_notification_done = true;
    if (operation->zero_copy_primary_done) {
      untrack_io_uring_operation(operation);
      destroy_io_uring_operation(operation);
    }
    return false;
  }

  const bool cqe_has_more = operation->multishot && result >= 0 &&
                            (cqe_flags & IORING_CQE_F_MORE) != 0U;
  const bool more = cqe_has_more && !operation->cancel_requested;
  const bool cancel_draining_more = cqe_has_more && operation->cancel_requested;
  const bool zero_copy_waits_for_notification =
      operation->zero_copy_send && (cqe_flags & IORING_CQE_F_MORE) != 0U;
  if (!more && !cancel_draining_more && !zero_copy_waits_for_notification) {
    untrack_io_uring_operation(operation);
  }
  operation->result->result = result;
  if (operation->cancel_requested) {
    if (result >= 0) {
      if (io_uring_operation_result_is_fd(operation)) {
        ::close(result);
      } else {
        clear_direct_io_uring_file_slot(operation);
      }
    }
    if (cancel_draining_more) {
      return false;
    }
    operation->result->events = io_error;
    operation->result->error = ECANCELED;
    operation->result->result = -ECANCELED;
  } else if (result < 0) {
    operation->result->events = io_error;
    operation->result->error = -result;
  } else {
    std::uint32_t events = operation->complete_events | (more ? io_more : 0U);
    if ((cqe_flags & IORING_CQE_F_BUFFER) != 0U) {
      events |= io_buffer_selected |
                ((cqe_flags >> io_buffer_id_shift) << io_buffer_id_shift);
    }
    operation->result->events = events;
    operation->result->error = 0;
    if (operation->msg != nullptr && operation->msg->address_size != nullptr) {
      *operation->msg->address_size = operation->msg->header.msg_namelen;
    }
    if (operation->opcode != IORING_OP_TIMEOUT &&
        operation->socket_address != nullptr &&
        operation->socket_address->output_size != nullptr) {
      const socklen_t actual_size = operation->socket_address->size;
      if (operation->socket_address->output != nullptr &&
          operation->socket_address->output_capacity != 0U) {
        const auto copy_size = static_cast<std::size_t>(
            std::min(actual_size, operation->socket_address->output_capacity));
        std::memcpy(operation->socket_address->output,
                    &operation->socket_address->storage, copy_size);
      }
      *operation->socket_address->output_size = actual_size;
    }
  }
  enqueue_pending_blocking(index_, operation->task);
  if (more) {
    return true;
  }
  if (zero_copy_waits_for_notification) {
    operation->zero_copy_primary_done = true;
    clear_io_uring_result_token(operation);
    operation->task = nullptr;
    operation->result = nullptr;
    if (operation->zero_copy_notification_done) {
      untrack_io_uring_operation(operation);
      destroy_io_uring_operation(operation);
    }
    return true;
  }
  destroy_io_uring_operation(operation);
  return false;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::complete_io_uring_poll_wait(
    IoUringOperation *operation, int result) noexcept {
  IoWaitRegistration *registration = operation->wait_registration;
  if (registration == nullptr || operation->task == nullptr ||
      operation->result == nullptr) {
    untrack_io_uring_operation(operation);
    destroy_io_uring_operation(operation);
    return;
  }

  const int fd = registration->fd;
  auto it = io_waits_.find(fd);
  if (it != io_waits_.end() && it->second == registration) {
    io_waits_.erase(it);
  }

  registration->result->fd = fd;
  registration->result->result = result;
  if (operation->cancel_requested) {
    registration->result->events = io_error;
    registration->result->error = ECANCELED;
    registration->result->result = -ECANCELED;
  } else if (result < 0) {
    registration->result->events = io_error;
    registration->result->error = -result;
  } else {
    registration->result->events =
        io_events_from_poll(static_cast<std::uint32_t>(result));
    registration->result->error = 0;
  }

  enqueue_pending_blocking(index_, registration->task);
  registration->poll_operation = nullptr;
  operation->wait_registration = nullptr;
  untrack_io_uring_operation(operation);
  destroy_io_uring_operation(operation);
  io_wait_pool_.destroy(registration);
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_uring_result_is_fd(
    std::uint8_t opcode) noexcept {
  return opcode == IORING_OP_OPENAT || opcode == IORING_OP_ACCEPT ||
         opcode == detail::io_uring_op_openat2 ||
         opcode == detail::io_uring_op_socket;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_uring_operation_result_is_fd(
    const IoUringOperation *operation) noexcept {
  return operation != nullptr && operation->direct_file_index < 0 &&
         io_uring_result_is_fd(operation->opcode);
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_direct_io_uring_file_slot(
    const IoUringOperation *operation) noexcept {
  if (operation == nullptr || operation->direct_file_index < 0 ||
      !io_uring_result_is_fd(operation->opcode) || io_uring_fd_ < 0 ||
      !io_uring_files_registered_) {
    return;
  }
  const int invalid_fd = -1;
  io_uring_files_update update{};
  update.offset = static_cast<unsigned>(operation->direct_file_index);
  update.fds = reinterpret_cast<std::uint64_t>(&invalid_fd);
  static_cast<void>(detail::sys_io_uring_register(
      io_uring_fd_, IORING_REGISTER_FILES_UPDATE, &update, 1));
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int Executor<RuntimeT, TraitsT>::submit_io_uring_cancel(
    IoUringOperation *operation) noexcept {
  int reserve_error = 0;
  io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
  if (sqe == nullptr) {
    return reserve_error == 0 ? EBUSY : reserve_error;
  }

  *sqe = io_uring_sqe{};
  sqe->opcode = IORING_OP_ASYNC_CANCEL;
  sqe->fd = -1;
  sqe->addr = reinterpret_cast<std::uint64_t>(operation);
  sqe->cancel_flags = 0;
  sqe->user_data = 0;

  const int submit_error = flush_io_uring_submissions();
  if (submit_error != 0) {
    fail_io_uring_backend(submit_error, nullptr);
  }
  return submit_error;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::track_io_uring_operation(
    IoUringOperation *operation) noexcept {
  operation->prev = nullptr;
  operation->next = io_uring_operations_;
  if (io_uring_operations_ != nullptr) {
    io_uring_operations_->prev = operation;
  }
  io_uring_operations_ = operation;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::untrack_io_uring_operation(
    IoUringOperation *operation) noexcept {
  if (operation->prev != nullptr) {
    operation->prev->next = operation->next;
  } else if (io_uring_operations_ == operation) {
    io_uring_operations_ = operation->next;
  }
  if (operation->next != nullptr) {
    operation->next->prev = operation->prev;
  }
  operation->prev = nullptr;
  operation->next = nullptr;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_io_uring_operations() noexcept {
  IoUringOperation *operation = io_uring_operations_;
  io_uring_operations_ = nullptr;
  while (operation != nullptr) {
    IoUringOperation *next = operation->next;
    operation->prev = nullptr;
    operation->next = nullptr;
    close_pending_io_uring_fd_result(operation);
    destroy_io_uring_operation(operation);
    operation = next;
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fail_io_uring_backend(
    int error, IoUringOperation *running_operation) noexcept {
  const int backend_error = error == 0 ? EIO : error;
  clear_or_fail_io_uring_operations(error, running_operation);
  close_io_uring_backend();
  io_uring_backend_error_ = backend_error;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_or_fail_io_uring_operations(
    int error, IoUringOperation *running_operation) noexcept {
  IoUringOperation *operation = io_uring_operations_;
  io_uring_operations_ = nullptr;
  while (operation != nullptr) {
    IoUringOperation *next = operation->next;
    operation->prev = nullptr;
    operation->next = nullptr;
    if (operation == running_operation) {
      close_pending_io_uring_fd_result(operation);
      destroy_io_uring_operation(operation);
      operation = next;
      continue;
    }

    close_pending_io_uring_fd_result(operation);
    if (operation->task == nullptr || operation->result == nullptr) {
      destroy_io_uring_operation(operation);
      operation = next;
      continue;
    }
    operation->result->events = io_error;
    operation->result->error = operation->cancel_requested ? ECANCELED : error;
    operation->result->result =
        operation->cancel_requested ? -ECANCELED : -error;
    enqueue_pending_blocking(index_, operation->task);
    destroy_io_uring_operation(operation);
    operation = next;
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_pending_io_uring_fd_result(
    IoUringOperation *operation) noexcept {
  if (operation == nullptr || operation->result == nullptr ||
      !io_uring_operation_result_is_fd(operation) ||
      operation->result->error != 0 ||
      (operation->result->events & operation->complete_events) == 0U ||
      operation->result->result < 0) {
    return;
  }
  ::close(static_cast<int>(operation->result->result));
  operation->result->events = io_error;
  operation->result->error = ECANCELED;
  operation->result->result = -ECANCELED;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::clear_io_uring_result_token(
    IoUringOperation *operation) noexcept {
  if (operation != nullptr && operation->result != nullptr &&
      operation->result->completion_token == operation) {
    operation->result->completion_token = nullptr;
  }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::destroy_io_uring_operation(
    IoUringOperation *operation) noexcept {
  clear_io_uring_result_token(operation);
  if (operation->msg != nullptr) {
    io_uring_msg_pool_.destroy(operation->msg);
    operation->msg = nullptr;
  }
  if (operation->opcode != IORING_OP_TIMEOUT &&
      operation->socket_address != nullptr) {
    io_uring_address_pool_.destroy(operation->socket_address);
    operation->socket_address = nullptr;
  }
  io_uring_op_pool_.destroy(operation);
}
#endif

} // namespace af::detail
