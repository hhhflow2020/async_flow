#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_io_backend.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::register_io_wait(int fd, std::uint32_t events,
                                                                 Task *task, IoResult *result,
                                                                 bool prefer_rearm) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_wait must be called from its IO thread");
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, fd, EINVAL);
        }
        return false;
    }

    return register_native_io_wait(fd, events, task, result, prefer_rearm);
}

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::cancel_io_completion(IoOpState &state) noexcept {
    if (io_uring_fd_ < 0) {
        state.wait.events = io_error;
        state.wait.error = ENOSYS;
        state.wait.result = -ENOSYS;
        return false;
    }

    auto *operation = static_cast<IoUringOperation *>(state.wait.completion_token);
    if (operation == nullptr || operation->result != &state.wait || operation->poll_wait) {
        state.wait.events = io_error;
        state.wait.error = ENOENT;
        state.wait.result = -ENOENT;
        return false;
    }
    if (operation->opcode == IORING_OP_CLOSE) {
        state.wait.events = io_error;
        state.wait.error = EOPNOTSUPP;
        state.wait.result = -EOPNOTSUPP;
        return false;
    }
    if (operation->cancel_requested) {
        return true;
    }

    const int submit_error = submit_io_uring_cancel(operation);
    if (submit_error != 0) {
        state.wait.events = io_error;
        state.wait.error = submit_error;
        state.wait.result = -submit_error;
        return false;
    }

    operation->cancel_requested = true;
    return true;
}
#endif

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::cancel_io(IoOpState &state) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "cancel_io must be called from its IO thread");
    if (state.waiting && state.wait.error == ECANCELED) {
        return true;
    }
    if (RuntimeT::current_thread_index_ != index_) {
        state.wait.events = io_error;
        state.wait.error = EINVAL;
        state.wait.result = -EINVAL;
        return false;
    }
    if (!state.waiting) {
        detail::set_io_result_error(state.wait, state.wait.fd, ENOENT);
        return false;
    }

#if defined(__linux__)
    if (state.wait_kind == IoWaitKind::Readiness) {
        return cancel_native_io_wait(state);
    }
    if (state.wait_kind == IoWaitKind::Completion) {
        return cancel_io_completion(state);
    }
#elif AF_DETAIL_HAS_NATIVE_IO_WAIT
    if (state.wait_kind == IoWaitKind::Readiness) {
        return cancel_native_io_wait(state);
    }
#if AF_DETAIL_HAS_KQUEUE
    if (state.wait_kind == IoWaitKind::Completion) {
        return cancel_kqueue_timeout(state);
    }
#endif
#endif
    state.wait.events = io_error;
    state.wait.error = ENOSYS;
    state.wait.result = -ENOSYS;
    return false;
}

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::io_wait_entry_empty(const IoWaitEntry &entry) noexcept {
    return entry.read == nullptr && entry.write == nullptr;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_wait_entry_contains(
    const IoWaitEntry &entry, const IoWaitRegistration *registration) noexcept {
    return entry.read == registration || entry.write == registration;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::io_wait_events_conflict(const IoWaitEntry &entry,
                                                     std::uint32_t events) noexcept {
    return ((events & io_readable) != 0U && entry.read != nullptr) ||
           ((events & io_writable) != 0U && entry.write != nullptr);
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::add_io_wait_registration(
    IoWaitEntry &entry, IoWaitRegistration *registration) noexcept {
    if ((registration->events & io_readable) != 0U) {
        entry.read = registration;
    }
    if ((registration->events & io_writable) != 0U) {
        entry.write = registration;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::remove_io_wait_registration(
    IoWaitEntry &entry, const IoWaitRegistration *registration) noexcept {
    if (entry.read == registration) {
        entry.read = nullptr;
    }
    if (entry.write == registration) {
        entry.write = nullptr;
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] typename Executor<RuntimeT, TraitsT>::IoWaitRegistration *
Executor<RuntimeT, TraitsT>::find_io_wait_registration(IoWaitEntry &entry,
                                                       const IoResult *result) noexcept {
    if (entry.read != nullptr && entry.read->result == result) {
        return entry.read;
    }
    if (entry.write != nullptr && entry.write != entry.read && entry.write->result == result) {
        return entry.write;
    }
    return nullptr;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::io_wait_registration_ready(const IoWaitRegistration &registration,
                                                        std::uint32_t ready_events) noexcept {
    if ((ready_events & (io_error | io_hangup)) != 0U) {
        return true;
    }
    return (registration.events & ready_events & (io_readable | io_writable)) != 0U;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_wait_registration_uses_native_backend(
    const IoWaitRegistration *registration) noexcept {
#if defined(__linux__)
    return registration != nullptr && registration->poll_operation == nullptr;
#else
    return registration != nullptr;
#endif
}
#endif

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
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
    bool did_work = poll_io_uring_completions();
#else
    bool did_work = false;
#endif
    return poll_native_io(timeout_ms, did_work);
}

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::reserve_io_backend_storage() noexcept {
    try {
        if constexpr (io_wait_reserve != 0U) {
            io_waits_.reserve(io_wait_reserve);
            io_wait_pool_.reserve_slots(io_wait_reserve);
        }
        if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
            io_uring_provided_buffer_groups_.reserve(io_uring_provided_buffer_group_reserve);
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
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_io_uring_completions() noexcept {
    if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr || io_uring_cq_tail_ == nullptr) {
        return false;
    }

    bool did_work = false;
    std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
    const std::uint32_t tail = __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
    while (head != tail) {
        io_uring_cqe &cqe = io_uring_cqes_[head & io_uring_cq_ring_mask_value_];
        auto *operation = reinterpret_cast<IoUringOperation *>(cqe.user_data);
        if (operation != nullptr) {
            const bool yield_to_task = complete_io_uring_operation(operation, cqe.res, cqe.flags);
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
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::complete_io_uring_operation(IoUringOperation *operation, int result,
                                                         std::uint32_t cqe_flags) noexcept {
    if (operation->net_poll) {
        complete_io_uring_net_poll(operation, result, cqe_flags);
        return false;
    }

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

    const bool cqe_has_more =
        operation->multishot && result >= 0 && (cqe_flags & IORING_CQE_F_MORE) != 0U;
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
        detail::set_io_result_error(*operation->result, operation->result->fd, ECANCELED);
    } else if (result < 0) {
        detail::set_io_result_error(*operation->result, operation->result->fd, -result);
    } else {
        std::uint32_t events = operation->complete_events | (more ? io_more : 0U);
        if ((cqe_flags & IORING_CQE_F_BUFFER) != 0U) {
            events |=
                io_buffer_selected | ((cqe_flags >> io_buffer_id_shift) << io_buffer_id_shift);
        }
        operation->result->events = events;
        operation->result->error = 0;
        if (operation->msg != nullptr && operation->msg->address_size != nullptr) {
            *operation->msg->address_size = operation->msg->header.msg_namelen;
        }
        if (operation->opcode != IORING_OP_TIMEOUT && operation->socket_address != nullptr &&
            operation->socket_address->output_size != nullptr) {
            const socklen_t actual_size = operation->socket_address->size;
            if (operation->socket_address->output != nullptr &&
                operation->socket_address->output_capacity != 0U) {
                const auto copy_size = static_cast<std::size_t>(
                    std::min(actual_size, operation->socket_address->output_capacity));
                std::memcpy(operation->socket_address->output, &operation->socket_address->storage,
                            copy_size);
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
void Executor<RuntimeT, TraitsT>::complete_io_uring_poll_wait(IoUringOperation *operation,
                                                              int result) noexcept {
    IoWaitRegistration *registration = operation->wait_registration;
    if (registration == nullptr || operation->task == nullptr || operation->result == nullptr) {
        untrack_io_uring_operation(operation);
        destroy_io_uring_operation(operation);
        return;
    }

    const int fd = registration->fd;
    auto it = io_waits_.find(fd);
    if (it != io_waits_.end() && io_wait_entry_contains(it->second, registration)) {
        remove_io_wait_registration(it->second, registration);
        if (io_wait_entry_empty(it->second)) {
            io_waits_.erase(it);
        }
    }

    registration->result->fd = fd;
    registration->result->result = result;
    if (operation->cancel_requested) {
        detail::set_io_result_error(*registration->result, fd, ECANCELED);
    } else if (result < 0) {
        detail::set_io_result_error(*registration->result, fd, -result);
    } else {
        registration->result->events = io_events_from_poll(static_cast<std::uint32_t>(result));
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
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::io_uring_result_is_fd(std::uint8_t opcode) noexcept {
    return opcode == IORING_OP_OPENAT || opcode == IORING_OP_ACCEPT ||
           opcode == detail::io_uring_op_openat2 || opcode == detail::io_uring_op_socket;
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
    static_cast<void>(
        detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_FILES_UPDATE, &update, 1));
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::track_io_uring_operation(IoUringOperation *operation) noexcept {
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
void Executor<RuntimeT, TraitsT>::untrack_io_uring_operation(IoUringOperation *operation) noexcept {
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

        if (fail_io_uring_net_poll(operation, error)) {
            destroy_io_uring_operation(operation);
            operation = next;
            continue;
        }

        if (fail_io_uring_poll_wait(operation, error)) {
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
        detail::set_io_result_error(*operation->result, operation->result->fd,
                                    operation->cancel_requested ? ECANCELED : error);
        enqueue_pending_blocking(index_, operation->task);
        destroy_io_uring_operation(operation);
        operation = next;
    }
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::fail_io_uring_poll_wait(IoUringOperation *operation,
                                                                        int error) noexcept {
    if (operation == nullptr || !operation->poll_wait) {
        return false;
    }

    const int completion_error =
        operation->cancel_requested ? ECANCELED : (error == 0 ? EIO : error);
    IoWaitRegistration *registration = operation->wait_registration;
    if (registration != nullptr) {
        const int fd = registration->fd;
        auto it = io_waits_.find(fd);
        if (it != io_waits_.end() && io_wait_entry_contains(it->second, registration)) {
            remove_io_wait_registration(it->second, registration);
            if (io_wait_entry_empty(it->second)) {
                io_waits_.erase(it);
            }
        }

        detail::set_io_result_error(*registration->result, fd, completion_error);
        if (registration->task != nullptr) {
            enqueue_pending_blocking(index_, registration->task);
        }
        registration->poll_operation = nullptr;
        operation->wait_registration = nullptr;
        io_wait_pool_.destroy(registration);
        return true;
    }

    if (operation->task != nullptr && operation->result != nullptr) {
        detail::set_io_result_error(*operation->result, operation->result->fd, completion_error);
        enqueue_pending_blocking(index_, operation->task);
    }
    return true;
}
#endif

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_pending_io_uring_fd_result(
    IoUringOperation *operation) noexcept {
    if (operation == nullptr || operation->result == nullptr ||
        !io_uring_operation_result_is_fd(operation) || operation->result->error != 0 ||
        (operation->result->events & operation->complete_events) == 0U ||
        operation->result->result < 0) {
        return;
    }
    ::close(static_cast<int>(operation->result->result));
    detail::set_io_result_error(*operation->result, operation->result->fd, ECANCELED);
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
void Executor<RuntimeT, TraitsT>::destroy_io_uring_operation(IoUringOperation *operation) noexcept {
    if (operation->net_poll && operation->net_channel != nullptr &&
        operation->net_channel->backend_token == operation) {
        operation->net_channel->backend_token = nullptr;
        operation->net_channel->active = false;
    }
    operation->net_channel = nullptr;
    clear_io_uring_result_token(operation);
    if (operation->msg != nullptr) {
        io_uring_msg_pool_.destroy(operation->msg);
        operation->msg = nullptr;
    }
    if (operation->opcode != IORING_OP_TIMEOUT && operation->socket_address != nullptr) {
        io_uring_address_pool_.destroy(operation->socket_address);
        operation->socket_address = nullptr;
    }
    io_uring_op_pool_.destroy(operation);
}
#endif

} // namespace af::detail
