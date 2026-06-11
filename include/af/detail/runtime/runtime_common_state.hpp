#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

#include "af/detail/config.hpp"
#include "af/detail/runtime/atomic_wait.hpp"

namespace af::detail {

template <typename T> struct alignas(hardware_cache_line_size) cache_line_atomic {
    std::atomic<T> value;

    constexpr cache_line_atomic() noexcept = default;
    constexpr explicit cache_line_atomic(T initial) noexcept : value(initial) {}

    cache_line_atomic(const cache_line_atomic &) = delete;
    cache_line_atomic &operator=(const cache_line_atomic &) = delete;

    [[nodiscard]] T load(std::memory_order order) const noexcept {
        return value.load(order);
    }

    void store(T desired, std::memory_order order) noexcept {
        value.store(desired, order);
    }

    T exchange(T desired, std::memory_order order) noexcept {
        return value.exchange(desired, order);
    }

    [[nodiscard]] bool compare_exchange_weak(T &expected, T desired, std::memory_order success,
                                             std::memory_order failure) noexcept {
        return value.compare_exchange_weak(expected, desired, success, failure);
    }

    [[nodiscard]] bool compare_exchange_strong(T &expected, T desired, std::memory_order success,
                                               std::memory_order failure) noexcept {
        return value.compare_exchange_strong(expected, desired, success, failure);
    }

    T fetch_add(T arg, std::memory_order order) noexcept {
        return value.fetch_add(arg, order);
    }

    T fetch_sub(T arg, std::memory_order order) noexcept {
        return value.fetch_sub(arg, order);
    }

    T fetch_and(T arg, std::memory_order order) noexcept {
        return value.fetch_and(arg, order);
    }

    void wait(T old, std::memory_order order) const noexcept {
        atomic_wait_value(value, old, order);
    }

    template <typename Rep, typename Period>
    bool wait_for(T old, std::chrono::duration<Rep, Period> timeout,
                  std::memory_order order) const noexcept {
        return atomic_wait_value_for(value, old, timeout, order);
    }

    void notify_one() noexcept {
        atomic_notify_one(value);
    }

    void notify_all() noexcept {
        atomic_notify_all(value);
    }
};

struct alignas(hardware_cache_line_size) ordered_batch_state {
    std::uint64_t last_applied_batch_id{0};
};

} // namespace af::detail
