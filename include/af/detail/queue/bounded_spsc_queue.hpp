#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <vector>

#include "af/detail/queue/bounded_queue_common.hpp"
#include "af/detail/config.hpp"

namespace af::detail {

template <typename T> class BoundedSpscQueue {
public:
    explicit BoundedSpscQueue(std::size_t capacity)
        : capacity_(normalize_bounded_queue_capacity(capacity)), mask_(capacity_ - 1),
          buffer_(capacity_) {}

    BoundedSpscQueue(const BoundedSpscQueue &) = delete;
    BoundedSpscQueue &operator=(const BoundedSpscQueue &) = delete;

    [[nodiscard]] bool try_push(T *value) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = tail + 1U;
        if (next - head_cache_ > capacity_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (next - head_cache_ > capacity_) {
                return false;
            }
        }

        buffer_[tail & mask_] = value;
        tail_.store(next, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::size_t try_push_many(T *const *values, std::size_t count) noexcept {
        if (count == 0U) {
            return 0;
        }

        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t used = tail - head_cache_;
        std::size_t available = used < capacity_ ? capacity_ - used : 0U;
        if (count > available) {
            head_cache_ = head_.load(std::memory_order_acquire);
            const std::size_t refreshed_used = tail - head_cache_;
            if (refreshed_used >= capacity_) {
                return 0;
            }
            available = capacity_ - refreshed_used;
            count = std::min(count, available);
        }

        const std::size_t first = tail & mask_;
        const std::size_t first_count = std::min(count, capacity_ - first);
        for (std::size_t i = 0; i < first_count; ++i) {
            buffer_[first + i] = values[i];
        }
        for (std::size_t i = first_count; i < count; ++i) {
            buffer_[i - first_count] = values[i];
        }

        tail_.store(tail + count, std::memory_order_release);
        return count;
    }

    [[nodiscard]] T *try_pop() noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return nullptr;
            }
        }

        T *value = buffer_[head & mask_];
        head_.store(head + 1U, std::memory_order_release);
        return value;
    }

    [[nodiscard]] std::size_t try_pop_many(T **out, std::size_t max_count) noexcept {
        if (max_count == 0U) {
            return 0;
        }

        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return 0;
            }
        } else if (tail_cache_ - head < max_count) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
        }

        const std::size_t available = tail_cache_ - head;
        const std::size_t count = available < max_count ? available : max_count;
        for (std::size_t i = 0; i < count; ++i) {
            out[i] = buffer_[(head + i) & mask_];
        }
        head_.store(head + count, std::memory_order_release);
        return count;
    }

private:
    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<T *> buffer_;

    alignas(hardware_cache_line_size) std::atomic<std::size_t> head_{0};
    alignas(hardware_cache_line_size) std::atomic<std::size_t> tail_{0};
    alignas(hardware_cache_line_size) std::size_t head_cache_{0};
    alignas(hardware_cache_line_size) std::size_t tail_cache_{0};
};

} // namespace af::detail
