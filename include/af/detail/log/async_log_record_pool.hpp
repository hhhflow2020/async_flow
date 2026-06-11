#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/async_log_config.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/queue/queue_backoff.hpp"
#include "af/span.hpp"

namespace af::detail {

enum class async_log_record_pool_kind : std::uint8_t {
    shared,
    Shared = shared,
};

struct async_log_record_pool_slot {
    void *owner{nullptr};
    async_log_record_pool_kind kind{async_log_record_pool_kind::shared};
};

class async_log_record_pool {
    static constexpr std::uint32_t null_slot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::size_t slot_index_bits = sizeof(std::uint32_t) * 8U;
    static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;
    inline static std::atomic<std::uint64_t> next_cache_token_{1U};

    struct Slab;

    struct alignas(hardware_cache_line_size) Slot : async_log_record_pool_slot {
        std::atomic<std::uint32_t> next{null_slot};
        log_record record;
        Slab *slab{nullptr};
        std::uint32_t index{0};
    };

    static_assert(alignof(Slot) >= hardware_cache_line_size,
                  "async log record pool slots must be cache-line aligned");
    static_assert(sizeof(Slot) % hardware_cache_line_size == 0U,
                  "async log record pool slots must not share cache lines");

    struct alignas(hardware_cache_line_size) Slab {
        Slab(async_log_record_pool *pool, std::size_t capacity)
            : capacity(validate_capacity(capacity)), slots(new Slot[this->capacity]) {
            for (std::size_t i = 0; i < this->capacity; ++i) {
                Slot &slot = slots[i];
                slot.owner = pool;
                slot.kind = async_log_record_pool_kind::shared;
                slot.slab = this;
                slot.index = static_cast<std::uint32_t>(i);
                slot.next.store(i + 1U < this->capacity ? static_cast<std::uint32_t>(i + 1U)
                                                        : null_slot,
                                std::memory_order_relaxed);
                slot.record.set_pool_slot(static_cast<async_log_record_pool_slot *>(&slot));
            }
            free_head.store(pack_head(0U, 0U), std::memory_order_relaxed);
        }

        const std::size_t capacity;
        std::unique_ptr<Slot[]> slots;
        std::atomic<Slab *> next{nullptr};
        alignas(hardware_cache_line_size) std::atomic<std::uint64_t> free_head{
            pack_head(null_slot, 0U)};
    };

public:
    explicit async_log_record_pool(std::size_t initial_capacity,
                                   std::size_t local_cache_capacity = 0U)
        : cache_token_(next_cache_token_.fetch_add(1U, std::memory_order_relaxed)),
          initial_slab_capacity_(validate_capacity(initial_capacity)),
          local_cache_capacity_(validate_local_cache_capacity(local_cache_capacity)),
          next_slab_capacity_(initial_slab_capacity_) {
        add_initial_slab();
    }

    async_log_record_pool(const async_log_record_pool &) = delete;
    async_log_record_pool &operator=(const async_log_record_pool &) = delete;

    ~async_log_record_pool() {
        lifetime_.reset();
    }

    [[nodiscard]] log_record *try_acquire(std::string_view message) noexcept {
        if (local_cache_capacity_ != 0U) [[likely]] {
            if (log_record *record = try_acquire_cached(message); record != nullptr) {
                return record;
            }
        }
        return try_acquire_uncached(message);
    }

    [[nodiscard]] log_record *try_acquire_uncached(std::string_view message) noexcept {
        for (;;) {
            bool reset_failed = false;
            if (log_record *record = try_acquire_from_existing(message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }

            Slab *slab = grow();
            if (slab == nullptr) {
                return nullptr;
            }
            reset_failed = false;
            if (log_record *record = try_acquire_from_slab(*slab, message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }
        }
    }

    static void release_slot(async_log_record_pool_slot *header) noexcept {
        auto *slot = static_cast<Slot *>(header);
        AF_ASSERT(slot != nullptr && slot->owner != nullptr && slot->slab != nullptr);
        static_cast<async_log_record_pool *>(slot->owner)->release(slot);
    }

    void release_records(af::span<log_record *const> records) noexcept {
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
    struct alignas(hardware_cache_line_size) LocalCache {
        async_log_record_pool *owner{nullptr};
        std::uint64_t owner_token{0};
        std::weak_ptr<void> owner_lifetime;
        std::array<Slot *, async_log_record_pool_max_local_cache_size> slots{};
        std::size_t capacity{0};
        std::size_t size{0};

        ~LocalCache() {
            flush();
        }

        [[nodiscard]] bool reset_for(async_log_record_pool *pool) noexcept {
            if (owner == pool && owner_token == pool->cache_token_) [[likely]] {
                return true;
            }
            if (owner == pool) [[unlikely]] {
                discard();
            } else {
                flush();
            }
            if (pool == nullptr || pool->local_cache_capacity_ == 0U) {
                capacity = 0;
                return false;
            }
            owner = pool;
            owner_token = pool->cache_token_;
            owner_lifetime = pool->lifetime_;
            capacity = pool->local_cache_capacity_;
            return true;
        }

        [[nodiscard]] Slot *pop() noexcept {
            if (size == 0U) [[unlikely]] {
                return nullptr;
            }
            return slots[--size];
        }

        [[nodiscard]] Slot **append_begin() noexcept {
            return slots.data() + size;
        }

        [[nodiscard]] std::size_t available() const noexcept {
            return capacity - size;
        }

        void append_commit(std::size_t count) noexcept {
            size += count;
        }

        void flush() noexcept {
            if (size != 0U) {
                if (auto alive = owner_lifetime.lock(); alive && owner != nullptr) {
                    owner->release_slots(slots.data(), size);
                }
            }
            size = 0;
            capacity = 0;
            owner = nullptr;
            owner_token = 0;
            owner_lifetime.reset();
        }

        void discard() noexcept {
            size = 0;
            capacity = 0;
            owner = nullptr;
            owner_token = 0;
            owner_lifetime.reset();
        }
    };

    [[nodiscard]] static std::size_t validate_capacity(std::size_t capacity) {
        if (capacity == 0U || capacity >= null_slot) {
            throw std::length_error("async log record pool capacity is out of range");
        }
        return capacity;
    }

    [[nodiscard]] static std::size_t validate_local_cache_capacity(std::size_t capacity) {
        if (capacity > async_log_record_pool_max_local_cache_size) {
            throw std::length_error("async log record pool local cache size is out of range");
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

    [[nodiscard]] static LocalCache &local_cache() noexcept {
        thread_local LocalCache cache;
        return cache;
    }

    [[nodiscard]] log_record *try_acquire_cached(std::string_view message) noexcept {
        LocalCache &cache = local_cache();
        if (!cache.reset_for(this)) [[unlikely]] {
            return nullptr;
        }

        Slot *slot = cache.pop();
        if (slot == nullptr) {
            refill_cache(cache);
            slot = cache.pop();
            if (slot == nullptr) {
                return nullptr;
            }
        }

        bool reset_failed = false;
        return prepare_record(*slot, message, reset_failed);
    }

    void refill_cache(LocalCache &cache) noexcept {
        const std::size_t available = cache.available();
        if (available == 0U) {
            return;
        }

        std::size_t count = try_pop_many_from_existing(cache.append_begin(), available);
        if (count == 0U) {
            if (Slab *slab = grow(); slab != nullptr) {
                count = try_pop_many(*slab, cache.append_begin(), available);
            }
        }
        cache.append_commit(count);
    }

    void add_initial_slab() {
        auto slab = std::make_unique<Slab>(this, initial_slab_capacity_);
        Slab *const raw = slab.get();
        slabs_.push_back(std::move(slab));
        slab_head_.store(raw, std::memory_order_release);
        next_slab_capacity_ = next_growth_capacity(initial_slab_capacity_);
    }

    [[nodiscard]] log_record *try_acquire_from_existing(std::string_view message,
                                                        bool &reset_failed) noexcept {
        Slab *slab = slab_head_.load(std::memory_order_acquire);
        while (slab != nullptr) {
            if (log_record *record = try_acquire_from_slab(*slab, message, reset_failed);
                record != nullptr || reset_failed) {
                return record;
            }
            slab = slab->next.load(std::memory_order_acquire);
        }
        return nullptr;
    }

    [[nodiscard]] log_record *try_acquire_from_slab(Slab &slab, std::string_view message,
                                                    bool &reset_failed) noexcept {
        Slot *slot = try_pop(slab);
        if (slot == nullptr) {
            return nullptr;
        }

        return prepare_record(*slot, message, reset_failed);
    }

    [[nodiscard]] log_record *prepare_record(Slot &slot, std::string_view message,
                                             bool &reset_failed) noexcept {
        try {
            slot.record.reset(message);
        } catch (...) {
            release(&slot);
            reset_failed = true;
            return nullptr;
        }
        return &slot.record;
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

    [[nodiscard]] std::size_t try_pop_many(Slab &slab, Slot **out, std::size_t max_count) noexcept {
        if (max_count == 0U) {
            return 0;
        }

        std::uint64_t head = slab.free_head.load(std::memory_order_acquire);
        for (;;) {
            std::uint32_t index = head_index(head);
            if (index == null_slot) {
                return 0;
            }

            std::size_t count = 0;
            while (count < max_count && index != null_slot) {
                Slot &slot = slab.slots[index];
                out[count] = &slot;
                ++count;
                index = slot.next.load(std::memory_order_relaxed);
            }

            const std::uint64_t desired = pack_head(index, head_version(head) + 1U);
            if (slab.free_head.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                     std::memory_order_acquire)) {
                return count;
            }
        }
    }

    [[nodiscard]] std::size_t try_pop_many_from_existing(Slot **out,
                                                         std::size_t max_count) noexcept {
        std::size_t count = 0;
        Slab *slab = slab_head_.load(std::memory_order_acquire);
        while (slab != nullptr && count < max_count) {
            count += try_pop_many(*slab, out + count, max_count - count);
            if (count != 0U) {
                return count;
            }
            slab = slab->next.load(std::memory_order_acquire);
        }
        return count;
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
        queue_full_backoff backoff(64U);
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

    void release_slots(Slot *const *slots, std::size_t count) noexcept {
        std::size_t begin = 0;
        while (begin < count) {
            Slot *first = slots[begin];
            AF_ASSERT(first != nullptr && first->owner == this && first->slab != nullptr);
            Slab *const slab = first->slab;

            std::size_t end = begin + 1U;
            while (end < count) {
                Slot *slot = slots[end];
                AF_ASSERT(slot != nullptr && slot->owner == this && slot->slab != nullptr);
                if (slot->slab != slab) {
                    break;
                }
                ++end;
            }

            release_slots_to_slab(*slab, slots + begin, end - begin);
            begin = end;
        }
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

    void release_slots_to_slab(Slab &slab, Slot *const *slots, std::size_t count) noexcept {
        if (count == 0U) {
            return;
        }
        if (count == 1U) {
            release(slots[0]);
            return;
        }

        Slot *first = slots[0];
        AF_ASSERT(first != nullptr && first->owner == this && first->slab == &slab);
        Slot *previous = first;
        for (std::size_t i = 1; i < count; ++i) {
            Slot *slot = slots[i];
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

    void release_records_to_slab(Slab &slab, af::span<log_record *const> records) noexcept {
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

    const std::uint64_t cache_token_;
    const std::size_t initial_slab_capacity_;
    const std::size_t local_cache_capacity_;
    std::shared_ptr<void> lifetime_{std::make_shared<std::uint8_t>(0)};
    std::size_t next_slab_capacity_;
    std::vector<std::unique_ptr<Slab>> slabs_;
    alignas(hardware_cache_line_size) std::atomic<Slab *> slab_head_{nullptr};
    alignas(hardware_cache_line_size) std::atomic_flag expanding_ = ATOMIC_FLAG_INIT;
};

inline void release_async_log_record(log_record *record) noexcept {
    auto *slot = static_cast<async_log_record_pool_slot *>(record->pool_slot());
    AF_ASSERT(slot != nullptr);
    switch (slot->kind) {
    case async_log_record_pool_kind::shared:
        async_log_record_pool::release_slot(slot);
        return;
    }
    AF_ASSERT(false);
}

inline void release_async_log_records(af::span<log_record *const> records) noexcept {
    std::size_t begin = 0;
    while (begin < records.size()) {
        auto *first_slot = static_cast<async_log_record_pool_slot *>(records[begin]->pool_slot());
        AF_ASSERT(first_slot != nullptr && first_slot->owner != nullptr);
        const async_log_record_pool_kind kind = first_slot->kind;
        void *const owner = first_slot->owner;

        std::size_t end = begin + 1U;
        while (end < records.size()) {
            auto *slot = static_cast<async_log_record_pool_slot *>(records[end]->pool_slot());
            AF_ASSERT(slot != nullptr && slot->owner != nullptr);
            if (slot->kind != kind || slot->owner != owner) {
                break;
            }
            ++end;
        }

        const auto group = records.subspan(begin, end - begin);
        switch (kind) {
        case async_log_record_pool_kind::shared:
            static_cast<async_log_record_pool *>(owner)->release_records(group);
            break;
        }
        begin = end;
    }
}

using AsyncLogRecordPoolKind = async_log_record_pool_kind;
using AsyncLogRecordPoolSlot = async_log_record_pool_slot;
using AsyncLogRecordPool = async_log_record_pool;

} // namespace af::detail
