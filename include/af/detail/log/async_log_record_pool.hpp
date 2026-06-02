#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "af/detail/config.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/queue/bounded_spsc_queue.hpp"

namespace af::detail {

enum class AsyncLogRecordPoolKind : std::uint8_t {
    Shared,
    Spsc,
};

struct AsyncLogRecordPoolSlot {
    void *owner{nullptr};
    AsyncLogRecordPoolKind kind{AsyncLogRecordPoolKind::Shared};
};

class AsyncLogRecordPool {
    static constexpr std::uint32_t null_slot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::size_t slot_index_bits = sizeof(std::uint32_t) * 8U;
    static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;

    struct Slot : AsyncLogRecordPoolSlot {
        std::atomic<std::uint32_t> next{null_slot};
        LogRecord record;
    };

public:
    explicit AsyncLogRecordPool(std::size_t capacity)
        : capacity_(validate_capacity(capacity)), slots_(new Slot[capacity_]) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            Slot &slot = slots_[i];
            slot.owner = this;
            slot.kind = AsyncLogRecordPoolKind::Shared;
            slot.next.store(i + 1U < capacity_ ? static_cast<std::uint32_t>(i + 1U) : null_slot,
                            std::memory_order_relaxed);
            slot.record.set_pool_slot(static_cast<AsyncLogRecordPoolSlot *>(&slot));
        }
        free_head_.store(pack_head(0U, 0U), std::memory_order_relaxed);
    }

    AsyncLogRecordPool(const AsyncLogRecordPool &) = delete;
    AsyncLogRecordPool &operator=(const AsyncLogRecordPool &) = delete;

    [[nodiscard]] LogRecord *try_acquire(std::string_view message) noexcept {
        Slot *slot = try_pop();
        if (slot == nullptr) {
            return nullptr;
        }

        try {
            slot->record.reset(message);
        } catch (...) {
            release(slot);
            return nullptr;
        }
        return &slot->record;
    }

    static void release_slot(AsyncLogRecordPoolSlot *header) noexcept {
        auto *slot = static_cast<Slot *>(header);
        AF_ASSERT(slot != nullptr && slot->owner != nullptr);
        static_cast<AsyncLogRecordPool *>(slot->owner)->release(slot);
    }

private:
    [[nodiscard]] static std::size_t validate_capacity(std::size_t capacity) {
        if (capacity == 0U || capacity >= null_slot) {
            throw std::length_error("async log record pool capacity is out of range");
        }
        return capacity;
    }

    [[nodiscard]] static constexpr std::uint64_t pack_head(std::uint32_t index,
                                                           std::uint64_t version) noexcept {
        return (version << slot_index_bits) | static_cast<std::uint64_t>(index);
    }

    [[nodiscard]] static constexpr std::uint32_t head_index(std::uint64_t head) noexcept {
        return static_cast<std::uint32_t>(head & slot_index_mask);
    }

    [[nodiscard]] static constexpr std::uint64_t head_version(std::uint64_t head) noexcept {
        return head >> slot_index_bits;
    }

    [[nodiscard]] std::uint32_t slot_index(const Slot *slot) const noexcept {
        const auto index = static_cast<std::size_t>(slot - slots_.get());
        AF_ASSERT(index < capacity_);
        return static_cast<std::uint32_t>(index);
    }

    [[nodiscard]] Slot *try_pop() noexcept {
        std::uint64_t head = free_head_.load(std::memory_order_acquire);
        for (;;) {
            const std::uint32_t index = head_index(head);
            if (index == null_slot) {
                return nullptr;
            }

            Slot &slot = slots_[index];
            const std::uint32_t next = slot.next.load(std::memory_order_relaxed);
            const std::uint64_t desired = pack_head(next, head_version(head) + 1U);
            if (free_head_.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                 std::memory_order_acquire)) {
                return &slot;
            }
        }
    }

    void release(Slot *slot) noexcept {
        const std::uint32_t index = slot_index(slot);
        std::uint64_t head = free_head_.load(std::memory_order_relaxed);
        for (;;) {
            slot->next.store(head_index(head), std::memory_order_relaxed);
            const std::uint64_t desired = pack_head(index, head_version(head) + 1U);
            if (free_head_.compare_exchange_weak(head, desired, std::memory_order_release,
                                                 std::memory_order_relaxed)) {
                return;
            }
        }
    }

    const std::size_t capacity_;
    std::unique_ptr<Slot[]> slots_;
    alignas(hardware_cache_line_size) std::atomic<std::uint64_t> free_head_{
        pack_head(null_slot, 0U)};
};

class AsyncLogSpscRecordPool {
    struct Slot : AsyncLogRecordPoolSlot {
        LogRecord record;
    };

public:
    explicit AsyncLogSpscRecordPool(std::size_t capacity)
        : capacity_(validate_capacity(capacity)), slots_(new Slot[capacity_]),
          free_slots_(capacity_) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            Slot &slot = slots_[i];
            slot.owner = this;
            slot.kind = AsyncLogRecordPoolKind::Spsc;
            slot.record.set_pool_slot(static_cast<AsyncLogRecordPoolSlot *>(&slot));
            const bool pushed = free_slots_.try_push(&slot);
            AF_ASSERT(pushed);
            static_cast<void>(pushed);
        }
    }

    AsyncLogSpscRecordPool(const AsyncLogSpscRecordPool &) = delete;
    AsyncLogSpscRecordPool &operator=(const AsyncLogSpscRecordPool &) = delete;

    [[nodiscard]] LogRecord *try_acquire(std::string_view message) noexcept {
        Slot *slot = free_slots_.try_pop();
        if (slot == nullptr) {
            return nullptr;
        }

        try {
            slot->record.reset(message);
        } catch (...) {
            release(slot);
            return nullptr;
        }
        return &slot->record;
    }

    static void release_slot(AsyncLogRecordPoolSlot *header) noexcept {
        auto *slot = static_cast<Slot *>(header);
        AF_ASSERT(slot != nullptr && slot->owner != nullptr);
        static_cast<AsyncLogSpscRecordPool *>(slot->owner)->release(slot);
    }

private:
    [[nodiscard]] static std::size_t validate_capacity(std::size_t capacity) {
        if (capacity == 0U || capacity >= std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("async log record pool capacity is out of range");
        }
        return capacity;
    }

    void release(Slot *slot) noexcept {
        const bool pushed = free_slots_.try_push(slot);
        AF_ASSERT(pushed);
        static_cast<void>(pushed);
    }

    const std::size_t capacity_;
    std::unique_ptr<Slot[]> slots_;
    BoundedSpscQueue<Slot> free_slots_;
};

inline void release_async_log_record(LogRecord *record) noexcept {
    auto *slot = static_cast<AsyncLogRecordPoolSlot *>(record->pool_slot());
    AF_ASSERT(slot != nullptr);
    switch (slot->kind) {
    case AsyncLogRecordPoolKind::Shared:
        AsyncLogRecordPool::release_slot(slot);
        return;
    case AsyncLogRecordPoolKind::Spsc:
        AsyncLogSpscRecordPool::release_slot(slot);
        return;
    }
    AF_ASSERT(false);
}

} // namespace af::detail
