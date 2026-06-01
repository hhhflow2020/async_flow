#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

#include "af/detail/bounded_queue_common.hpp"
#include "af/detail/config.hpp"

namespace af::detail {

template <typename T> class BoundedSpscQueue {
public:
    explicit BoundedSpscQueue(std::size_t capacity)
        : capacity_(next_power_of_two(capacity < 2 ? 2 : capacity)), mask_(capacity_ - 1),
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
