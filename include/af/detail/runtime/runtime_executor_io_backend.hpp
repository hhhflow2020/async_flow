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

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    if (state.wait_kind == IoWaitKind::Readiness) {
        return cancel_native_io_wait(state);
    }
#if AF_DETAIL_HAS_KQUEUE
    if (state.wait_kind == IoWaitKind::Timer) {
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
    return registration != nullptr;
}
#endif

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::init_io_backend() noexcept {
    if (!io_thread() || native_io_backend_available()) {
        return;
    }
    static_cast<void>(init_native_io_backend());
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::close_io_backend() noexcept {
    close_native_io_backend();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::poll_io(int timeout_ms) noexcept {
    return poll_native_io(timeout_ms, false);
}

#if AF_DETAIL_HAS_EPOLL
template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::reserve_io_backend_storage() noexcept {
    try {
        if constexpr (io_wait_reserve != 0U) {
            io_waits_.reserve(io_wait_reserve);
            io_wait_pool_.reserve_slots(io_wait_reserve);
        }
    } catch (...) {
    }
}
#endif

} // namespace af::detail
