#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/queue/queue_backoff.hpp"
#include "af/span.hpp"

namespace af::detail {

enum class AsyncLogRecordPoolKind : std::uint8_t {
    Shared,
};

struct AsyncLogRecordPoolSlot {
    void *owner{nullptr};
    AsyncLogRecordPoolKind kind{AsyncLogRecordPoolKind::Shared};
};

class AsyncLogRecordPool {
    static constexpr std::uint32_t null_slot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::size_t slot_index_bits = sizeof(std::uint32_t) * 8U;
    static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;

    struct Slab;

    struct Slot : AsyncLogRecordPoolSlot {
        std::atomic<std::uint32_t> next{null_slot};
        LogRecord record;
        Slab *slab{nullptr};
        std::uint32_t index{0};
    };

    struct alignas(hardware_cache_line_size) Slab {
        Slab(AsyncLogRecordPool *pool, std::size_t capacity)
            : capacity(validate_capacity(capacity)), slots(new Slot[this->capacity]) {
            for (std::size_t i = 0; i < this->capacity; ++i) {
                Slot &slot = slots[i];
                slot.owner = pool;
                slot.kind = AsyncLogRecordPoolKind::Shared;
                slot.slab = this;
                slot.index = static_cast<std::uint32_t>(i);
                slot.next.store(i + 1U < this->capacity ? static_cast<std::uint32_t>(i + 1U)
                                                        : null_slot,
                                std::memory_order_relaxed);
                slot.record.set_pool_slot(static_cast<AsyncLogRecordPoolSlot *>(&slot));
            }
            free_head.store(pack_head(0U, 0U), std::memory_order_relaxed);
        }

        const std::size_t capacity;
        std::unique_ptr<Slot[]> slots;
        alignas(hardware_cache_line_size) std::atomic<std::uint64_t> free_head{
            pack_head(null_slot, 0U)};
        std::atomic<Slab *> next{nullptr};
    };

public:
    explicit AsyncLogRecordPool(std::size_t initial_capacity)
        : initial_slab_capacity_(validate_capacity(initial_capacity)),
          next_slab_capacity_(initial_slab_capacity_) {
        add_initial_slab();
    }

    AsyncLogRecordPool(const AsyncLogRecordPool &) = delete;
    AsyncLogRecordPool &operator=(const AsyncLogRecordPool &) = delete;

    [[nodiscard]] LogRecord *try_acquire(std::string_view message) noexcept {
        for (;;) {
            bool reset_failed = false;
            if (LogRecord *record = try_acquire_from_existing(message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }

            Slab *slab = grow();
            if (slab == nullptr) {
                return nullptr;
            }
            reset_failed = false;
            if (LogRecord *record = try_acquire_from_slab(*slab, message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }
        }
    }

    static void release_slot(AsyncLogRecordPoolSlot *header) noexcept {
        auto *slot = static_cast<Slot *>(header);
        AF_ASSERT(slot != nullptr && slot->owner != nullptr && slot->slab != nullptr);
        static_cast<AsyncLogRecordPool *>(slot->owner)->release(slot);
    }

    void release_records(af::Span<LogRecord *const> records) noexcept {
        std::size_t begin = 0;
        while (begin < records.size()) {
            auto *first = static_cast<Slot *>(records[begin]->pool_slot());
            AF_ASSERT(first != nullptr && first->owner == this && first->slab != nullptr);
            Slab *const slab = first->slab;

            std::size_t end = begin + 1U;
            while (end < records.size()) {
                auto *slot = static_cast<Slot *>(records[end]->pool_slot());
                AF_ASSERT(slot != nullptr && slot->owner == this && slot->slab != nullptr);
                if (slot->slab != slab) {
                    break;
                }
                ++end;
            }

            release_records_to_slab(*slab, records.subspan(begin, end - begin));
            begin = end;
        }
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

    [[nodiscard]] static std::size_t next_growth_capacity(std::size_t current) noexcept {
        constexpr std::size_t max_growth = 1U << 20U;
        if (current >= max_growth) {
            return current;
        }
        const std::size_t doubled = current * 2U;
        return doubled < current ? max_growth : (doubled > max_growth ? max_growth : doubled);
    }

    void add_initial_slab() {
        auto slab = std::make_unique<Slab>(this, initial_slab_capacity_);
        Slab *const raw = slab.get();
        slabs_.push_back(std::move(slab));
        slab_head_.store(raw, std::memory_order_release);
        next_slab_capacity_ = next_growth_capacity(initial_slab_capacity_);
    }

    [[nodiscard]] LogRecord *try_acquire_from_existing(std::string_view message,
                                                       bool &reset_failed) noexcept {
        Slab *slab = slab_head_.load(std::memory_order_acquire);
        while (slab != nullptr) {
            if (LogRecord *record = try_acquire_from_slab(*slab, message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }
            slab = slab->next.load(std::memory_order_acquire);
        }
        return nullptr;
    }

    [[nodiscard]] LogRecord *try_acquire_from_slab(Slab &slab, std::string_view message,
                                                   bool &reset_failed) noexcept {
        Slot *slot = try_pop(slab);
        if (slot == nullptr) {
            return nullptr;
        }

        try {
            slot->record.reset(message);
        } catch (...) {
            release(slot);
            reset_failed = true;
            return nullptr;
        }
        return &slot->record;
    }

    [[nodiscard]] Slot *try_pop(Slab &slab) noexcept {
        std::uint64_t head = slab.free_head.load(std::memory_order_acquire);
        for (;;) {
            const std::uint32_t index = head_index(head);
            if (index == null_slot) {
                return nullptr;
            }

            Slot &slot = slab.slots[index];
            const std::uint32_t next = slot.next.load(std::memory_order_relaxed);
            const std::uint64_t desired = pack_head(next, head_version(head) + 1U);
            if (slab.free_head.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                     std::memory_order_acquire)) {
                return &slot;
            }
        }
    }

    [[nodiscard]] Slab *find_slab_with_free_slot() noexcept {
        Slab *slab = slab_head_.load(std::memory_order_acquire);
        while (slab != nullptr) {
            const std::uint64_t head = slab->free_head.load(std::memory_order_acquire);
            if (head_index(head) != null_slot) {
                return slab;
            }
            slab = slab->next.load(std::memory_order_acquire);
        }
        return nullptr;
    }

    [[nodiscard]] Slab *grow() noexcept {
        QueueFullBackoff backoff(64U);
        while (expanding_.test_and_set(std::memory_order_acquire)) {
            backoff.wait();
        }

        Slab *published = nullptr;
        try {
            published = find_slab_with_free_slot();
            if (published == nullptr) {
                auto slab = std::make_unique<Slab>(this, next_slab_capacity_);
                published = slab.get();
                slabs_.push_back(std::move(slab));

                Slab *head = slab_head_.load(std::memory_order_acquire);
                do {
                    published->next.store(head, std::memory_order_relaxed);
                } while (!slab_head_.compare_exchange_weak(
                    head, published, std::memory_order_release, std::memory_order_acquire));
                next_slab_capacity_ = next_growth_capacity(next_slab_capacity_);
            }
        } catch (...) {
            published = nullptr;
        }

        expanding_.clear(std::memory_order_release);
        return published;
    }

    void release(Slot *slot) noexcept {
        AF_ASSERT(slot != nullptr && slot->slab != nullptr);
        Slab &slab = *slot->slab;
        const std::uint32_t index = slot->index;
        std::uint64_t head = slab.free_head.load(std::memory_order_relaxed);
        for (;;) {
            slot->next.store(head_index(head), std::memory_order_relaxed);
            const std::uint64_t desired = pack_head(index, head_version(head) + 1U);
            if (slab.free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                return;
            }
        }
    }

    void release_records_to_slab(Slab &slab, af::Span<LogRecord *const> records) noexcept {
        if (records.empty()) {
            return;
        }
        if (records.size() == 1U) {
            release(static_cast<Slot *>(records.front()->pool_slot()));
            return;
        }

        Slot *first = static_cast<Slot *>(records.front()->pool_slot());
        AF_ASSERT(first != nullptr && first->owner == this && first->slab == &slab);
        Slot *previous = first;
        for (std::size_t i = 1; i < records.size(); ++i) {
            auto *slot = static_cast<Slot *>(records[i]->pool_slot());
            AF_ASSERT(slot != nullptr && slot->owner == this && slot->slab == &slab);
            previous->next.store(slot->index, std::memory_order_relaxed);
            previous = slot;
        }

        const std::uint32_t first_index = first->index;
        std::uint64_t head = slab.free_head.load(std::memory_order_relaxed);
        for (;;) {
            previous->next.store(head_index(head), std::memory_order_relaxed);
            const std::uint64_t desired = pack_head(first_index, head_version(head) + 1U);
            if (slab.free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                return;
            }
        }
    }

    const std::size_t initial_slab_capacity_;
    std::size_t next_slab_capacity_;
    std::vector<std::unique_ptr<Slab>> slabs_;
    alignas(hardware_cache_line_size) std::atomic<Slab *> slab_head_{nullptr};
    alignas(hardware_cache_line_size) std::atomic_flag expanding_ = ATOMIC_FLAG_INIT;
};

inline void release_async_log_record(LogRecord *record) noexcept {
    auto *slot = static_cast<AsyncLogRecordPoolSlot *>(record->pool_slot());
    AF_ASSERT(slot != nullptr);
    switch (slot->kind) {
    case AsyncLogRecordPoolKind::Shared:
        AsyncLogRecordPool::release_slot(slot);
        return;
    }
    AF_ASSERT(false);
}

inline void release_async_log_records(af::Span<LogRecord *const> records) noexcept {
    std::size_t begin = 0;
    while (begin < records.size()) {
        auto *first_slot = static_cast<AsyncLogRecordPoolSlot *>(records[begin]->pool_slot());
        AF_ASSERT(first_slot != nullptr && first_slot->owner != nullptr);
        const AsyncLogRecordPoolKind kind = first_slot->kind;
        void *const owner = first_slot->owner;

        std::size_t end = begin + 1U;
        while (end < records.size()) {
            auto *slot = static_cast<AsyncLogRecordPoolSlot *>(records[end]->pool_slot());
            AF_ASSERT(slot != nullptr && slot->owner != nullptr);
            if (slot->kind != kind || slot->owner != owner) {
                break;
            }
            ++end;
        }

        const auto group = records.subspan(begin, end - begin);
        switch (kind) {
        case AsyncLogRecordPoolKind::Shared:
            static_cast<AsyncLogRecordPool *>(owner)->release_records(group);
            break;
        }
        begin = end;
    }
}

} // namespace af::detail
