#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_lifecycle.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
Executor<RuntimeT, TraitsT>::Executor(std::uint16_t index)
    : index_(index), kind_(thread_kind(thread_from_index(index))),
      local_queue_(detail::next_power_of_two(spsc_queue_capacity < 2 ? 2 : spsc_queue_capacity)) {}

template <typename RuntimeT, typename TraitsT> Executor<RuntimeT, TraitsT>::~Executor() {
    close_io_backend();
}

template <typename RuntimeT, typename TraitsT> void Executor<RuntimeT, TraitsT>::start() {
    init_io_backend();
    worker_ = std::thread([this] { run_loop(); });
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

template <typename RuntimeT, typename TraitsT> void Executor<RuntimeT, TraitsT>::notify() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (!io_thread() || !io_backend_available()) {
        wake_epoch_.notify_one();
        return;
    }

    if (!sleeping_.load(std::memory_order_acquire)) {
        return;
    }

    bool expected = true;
    if (sleeping_.compare_exchange_strong(expected, false, std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
        if (notify_native_io_backend()) {
            return;
        }
        wake_epoch_.notify_one();
    }
}

} // namespace af::detail
