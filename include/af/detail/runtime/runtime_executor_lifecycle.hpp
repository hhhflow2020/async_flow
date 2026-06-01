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
#if !defined(_WIN32)
    std::array<char, 16> name{};
    const Thread thread = thread_from_index(index_);
    const std::string_view group_name = thread_name(thread);
    const std::uint16_t group_offset = thread_group_offset(thread);
    const auto offset = static_cast<unsigned>(group_offset);
    const int offset_digits = std::snprintf(nullptr, 0, "%u", offset);
    if (offset_digits <= 0) {
        return;
    }
    constexpr std::size_t max_name_chars = 15;
    const std::size_t fixed_chars = 4U + static_cast<std::size_t>(offset_digits);
    std::size_t group_chars = 0;
    if (fixed_chars < max_name_chars) {
        group_chars = max_name_chars - fixed_chars;
        if (group_chars > group_name.size()) {
            group_chars = group_name.size();
        }
    }
    const int written = std::snprintf(name.data(), name.size(), "af-%.*s-%u",
                                      static_cast<int>(group_chars), group_name.data(), offset);
    if (written <= 0) {
        return;
    }
#if defined(__APPLE__)
    static_cast<void>(::pthread_setname_np(name.data()));
#elif defined(__linux__)
    static_cast<void>(::pthread_setname_np(::pthread_self(), name.data()));
#endif
#endif
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
