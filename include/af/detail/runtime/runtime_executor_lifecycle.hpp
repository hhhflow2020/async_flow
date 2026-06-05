#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_lifecycle.hpp must be included by async_runtime.hpp"
#endif

#include "af/detail/thread/thread_name.hpp"

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
Executor<RuntimeT, TraitsT>::Executor(std::uint16_t index)
    : index_(index), kind_(thread_kind(thread_from_index(index))) {
    timers_.reserve(timer_reserve);
}

template <typename RuntimeT, typename TraitsT> Executor<RuntimeT, TraitsT>::~Executor() {
    close_io_backend();
}

template <typename RuntimeT, typename TraitsT> void Executor<RuntimeT, TraitsT>::start() {
    init_io_backend();
    worker_ = std::thread([this] {
        set_current_thread_name();
        run_loop();
    });
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    notify_force();
}

template <typename RuntimeT, typename TraitsT> void Executor<RuntimeT, TraitsT>::join() {
    if (worker_.joinable()) {
        worker_.join();
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::set_current_thread_name() noexcept {
    const Thread thread = thread_from_index(index_);
    af::detail::set_current_thread_name(thread_name(thread), thread_group_offset(thread));
}

template <typename RuntimeT, typename TraitsT> void Executor<RuntimeT, TraitsT>::notify() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    const bool was_sleeping = sleeping_.exchange(false, std::memory_order_acq_rel);

    if (was_sleeping && io_thread() && io_backend_available() && notify_native_io_backend()) {
        return;
    }
    wake_epoch_.notify_one();
}

} // namespace af::detail
