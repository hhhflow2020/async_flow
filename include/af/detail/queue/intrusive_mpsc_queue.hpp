#pragma once

#include <atomic>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename T> class IntrusiveMpscQueue {
public:
    IntrusiveMpscQueue() = default;
    IntrusiveMpscQueue(const IntrusiveMpscQueue &) = delete;
    IntrusiveMpscQueue &operator=(const IntrusiveMpscQueue &) = delete;

    void push(T *value) noexcept {
        AF_ASSERT(value != nullptr);
        next(value).store(nullptr, std::memory_order_relaxed);
        T *head = head_.load(std::memory_order_relaxed);
        do {
            next(value).store(head, std::memory_order_relaxed);
        } while (!head_.compare_exchange_weak(head, value, std::memory_order_release,
                                              std::memory_order_relaxed));
    }

    [[nodiscard]] T *try_pop() noexcept {
        if (consumer_head_ == nullptr) {
            refill_consumer_cache();
        }
        if (consumer_head_ == nullptr) {
            return nullptr;
        }

        T *value = consumer_head_;
        consumer_head_ = next(value).load(std::memory_order_relaxed);
        next(value).store(nullptr, std::memory_order_relaxed);
        return value;
    }

    [[nodiscard]] bool empty() const noexcept {
        return consumer_head_ == nullptr && head_.load(std::memory_order_acquire) == nullptr;
    }

private:
    [[nodiscard]] static std::atomic<T *> &next(T *value) noexcept {
        return value->intrusive_mpsc_next_;
    }

    void refill_consumer_cache() noexcept {
        T *list = head_.exchange(nullptr, std::memory_order_acquire);
        T *reversed = nullptr;
        while (list != nullptr) {
            T *next_value = next(list).load(std::memory_order_relaxed);
            next(list).store(reversed, std::memory_order_relaxed);
            reversed = list;
            list = next_value;
        }
        consumer_head_ = reversed;
    }

    alignas(hardware_cache_line_size) std::atomic<T *> head_{nullptr};
    alignas(hardware_cache_line_size) T *consumer_head_{nullptr};
};

} // namespace af::detail
