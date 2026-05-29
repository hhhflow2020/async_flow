#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "caf/detail/config.hpp"

namespace caf::detail {

inline constexpr std::size_t next_power_of_two(std::size_t value) noexcept {
    std::size_t result = 1;
    while (result < value) {
        result <<= 1U;
    }
    return result;
}

template <typename T>
class BoundedSpscQueue {
public:
    explicit BoundedSpscQueue(std::size_t capacity)
        : capacity_(next_power_of_two(capacity < 2 ? 2 : capacity)),
          mask_(capacity_ - 1),
          buffer_(capacity_) {}

    BoundedSpscQueue(const BoundedSpscQueue&) = delete;
    BoundedSpscQueue& operator=(const BoundedSpscQueue&) = delete;

    [[nodiscard]] bool try_push(T* value) noexcept {
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

    [[nodiscard]] T* try_pop() noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (tail_cache_ == head) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (tail_cache_ == head) {
                return nullptr;
            }
        }

        T* value = buffer_[head & mask_];
        head_.store(head + 1U, std::memory_order_release);
        return value;
    }

private:
    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<T*> buffer_;

    alignas(hardware_cache_line_size) std::atomic<std::size_t> head_{0};
    alignas(hardware_cache_line_size) std::atomic<std::size_t> tail_{0};
    alignas(hardware_cache_line_size) std::size_t head_cache_{0};
    alignas(hardware_cache_line_size) std::size_t tail_cache_{0};
};

template <typename T>
class BoundedMpmcQueue {
public:
    explicit BoundedMpmcQueue(std::size_t capacity)
        : capacity_(next_power_of_two(capacity < 2 ? 2 : capacity)),
          mask_(capacity_ - 1),
          buffer_(std::make_unique<Cell[]>(capacity_)) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    BoundedMpmcQueue(const BoundedMpmcQueue&) = delete;
    BoundedMpmcQueue& operator=(const BoundedMpmcQueue&) = delete;

    [[nodiscard]] bool try_push(T* value) noexcept {
        Cell* cell = nullptr;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            if (diff == 0) {
                if (enqueue_pos_.compare_exchange_weak(
                        pos,
                        pos + 1U,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        cell->data = value;
        cell->sequence.store(pos + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] T* try_pop() noexcept {
        Cell* cell = nullptr;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1U);
            if (diff == 0) {
                if (dequeue_pos_.compare_exchange_weak(
                        pos,
                        pos + 1U,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return nullptr;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        T* value = cell->data;
        cell->sequence.store(pos + capacity_, std::memory_order_release);
        return value;
    }

private:
    struct alignas(hardware_cache_line_size) Cell {
        std::atomic<std::size_t> sequence{0};
        T* data{nullptr};
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> buffer_;

    alignas(hardware_cache_line_size) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(hardware_cache_line_size) std::atomic<std::size_t> dequeue_pos_{0};
};

} // namespace caf::detail
