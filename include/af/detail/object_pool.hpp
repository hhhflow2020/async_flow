#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename T, std::size_t ChunkSize = 512, std::size_t RemoteReleaseBatchSize = 1,
          bool CacheAllocatedSlotIndex = false, std::size_t LocalCacheSetSize = 8,
          std::size_t DirectReleaseSetSize = 4, std::size_t LocalCacheCapacity = 64>
class ObjectPool {
    static_assert(ChunkSize > 0, "ObjectPool chunk size must be greater than zero");
    static_assert(RemoteReleaseBatchSize > 0,
                  "ObjectPool remote release batch size must be greater than zero");

public:
    ObjectPool() = default;
    ObjectPool(const ObjectPool &) = delete;
    ObjectPool &operator=(const ObjectPool &) = delete;

    ~ObjectPool() {
        tls_caches().discard_if_owner(this);
        Block *block = blocks_.load(std::memory_order_relaxed);
        while (block != nullptr) {
            Block *next = block->next;
            delete block;
            block = next;
        }
    }

    template <typename... Args> [[nodiscard]] T *create(Args &&...args) {
        void *memory = acquire_slot();
        if constexpr (std::is_nothrow_constructible_v<T, Args...>) {
            return std::construct_at(static_cast<T *>(memory), std::forward<Args>(args)...);
        } else {
            try {
                return std::construct_at(static_cast<T *>(memory), std::forward<Args>(args)...);
            } catch (...) {
                release_slot(memory);
                throw;
            }
        }
    }

    void destroy(T *object) noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::destroy_at(object);
        }
        release_slot(object);
    }

    void reserve_slots(std::size_t slot_count) {
        reserve_blocks(slot_count == 0U ? 0U : ((slot_count - 1U) / ChunkSize) + 1U);
    }

    void reserve_blocks(std::size_t block_count) {
        while (block_count_.load(std::memory_order_acquire) < block_count) {
            static_cast<void>(add_block());
        }
    }

private:
    static constexpr std::size_t local_cache_capacity = LocalCacheCapacity;
    static constexpr std::size_t local_cache_flush_count = local_cache_capacity / 2;
    static constexpr std::size_t remote_release_batch_size = RemoteReleaseBatchSize;
    static constexpr bool cache_allocated_slot_index = CacheAllocatedSlotIndex;
    static constexpr std::size_t local_cache_set_size = LocalCacheSetSize;
    static constexpr std::size_t direct_release_set_size = DirectReleaseSetSize;
    static_assert(local_cache_set_size >= 1);
    static_assert(direct_release_set_size >= 1);
    static_assert(local_cache_capacity > 0);
    static_assert(local_cache_flush_count > 0);
    static_assert(local_cache_flush_count <= local_cache_capacity);
    static_assert(remote_release_batch_size <= local_cache_capacity);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "ObjectPool tagged free stack requires lock-free 64-bit atomics");
    static_assert(std::atomic<void *>::is_always_lock_free,
                  "ObjectPool block list requires lock-free pointer atomics");
    using slot_index_type = std::uint16_t;
    static_assert(std::atomic<slot_index_type>::is_always_lock_free,
                  "ObjectPool free-list slot links require lock-free index atomics");
    static constexpr slot_index_type null_slot_index = std::numeric_limits<slot_index_type>::max();
    static_assert(ChunkSize < static_cast<std::size_t>(null_slot_index),
                  "ObjectPool chunk size must leave 48 bits for the free-list ABA tag");

    static constexpr std::size_t slot_index_bits = sizeof(slot_index_type) * 8U;
    static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;

    static constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
        return ((value + alignment - 1U) / alignment) * alignment;
    }

    static constexpr std::size_t slot_align = alignof(T) > hardware_cache_line_size
                                                  ? alignof(T)
                                                  : hardware_cache_line_size;
    static constexpr std::size_t slot_header_size =
        sizeof(void *) + sizeof(std::atomic<slot_index_type>);
    static constexpr std::size_t compact_storage_offset = align_up(slot_header_size, alignof(T));
    static constexpr bool compact_slot =
        compact_storage_offset + sizeof(T) <= hardware_cache_line_size;
    static constexpr std::size_t noncompact_storage_offset =
        align_up(hardware_cache_line_size, alignof(T));
    static constexpr std::size_t storage_offset =
        compact_slot ? compact_storage_offset : noncompact_storage_offset;
    static constexpr std::size_t storage_align = compact_slot ? alignof(T) : slot_align;
    static constexpr std::size_t slot_payload_size = storage_offset + sizeof(T);
    static constexpr std::size_t slot_size = align_up(slot_payload_size, slot_align);
    static constexpr std::size_t storage_size = slot_size - storage_offset;

    struct Block;

    struct alignas(slot_align) Slot {
        Block *owner{nullptr};
        std::atomic<slot_index_type> next_free{null_slot_index};
        alignas(storage_align) std::byte storage[storage_size];
    };

    static_assert(std::is_standard_layout_v<Slot>,
                  "ObjectPool slot lookup requires standard-layout slots");
    static_assert(offsetof(Slot, storage) == storage_offset,
                  "ObjectPool slot storage offset must match computed layout");
    static_assert(offsetof(Slot, storage) % alignof(T) == 0U,
                  "ObjectPool slot storage must satisfy T alignment");
    static_assert(sizeof(Slot) % hardware_cache_line_size == 0U,
                  "ObjectPool slots must not share cache lines");

    struct Block {
        explicit Block() {
            for (std::size_t i = 0; i < ChunkSize; ++i) {
                Slot &slot = slots[i];
                slot.owner = this;
                const auto next =
                    i + 1U < ChunkSize ? static_cast<slot_index_type>(i + 1U) : null_slot_index;
                slot.next_free.store(next, std::memory_order_relaxed);
            }
            free_head.store(pack_free_head(0U, 0U), std::memory_order_relaxed);
        }

        [[nodiscard]] std::size_t try_pop_many(Slot **out, std::size_t max_count) noexcept {
            if (max_count == 0U) {
                return 0;
            }
            AF_ASSERT(max_count <= local_cache_capacity);
            slot_index_type popped_indices[cache_allocated_slot_index ? local_cache_capacity : 1U];
            std::uint64_t head = free_head.load(std::memory_order_acquire);
            for (;;) {
                slot_index_type index = free_head_index(head);
                if (index == null_slot_index) {
                    return 0;
                }

                std::size_t count = 0;
                while (count < max_count && index != null_slot_index) {
                    Slot &slot = slots[index];
                    out[count] = &slot;
                    if constexpr (cache_allocated_slot_index) {
                        popped_indices[count] = index;
                    }
                    ++count;
                    index = slot.next_free.load(std::memory_order_relaxed);
                }

                const std::uint64_t desired = pack_free_head(index, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                    std::memory_order_acquire)) {
                    if constexpr (cache_allocated_slot_index) {
                        for (std::size_t i = 0; i < count; ++i) {
                            out[i]->next_free.store(popped_indices[i], std::memory_order_relaxed);
                        }
                    }
                    return count;
                }
            }
        }

        void push(Slot *slot) noexcept {
            const slot_index_type index = released_slot_index(slot);
            std::uint64_t head = free_head.load(std::memory_order_relaxed);
            for (;;) {
                slot->next_free.store(free_head_index(head), std::memory_order_relaxed);
                const std::uint64_t desired = pack_free_head(index, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                    return;
                }
            }
        }

        void push_many(Slot **pushed_slots, std::size_t count) noexcept {
            AF_ASSERT(count != 0U);
            const slot_index_type first = released_slot_index(pushed_slots[0]);
            for (std::size_t i = 1; i < count; ++i) {
                pushed_slots[i - 1]->next_free.store(released_slot_index(pushed_slots[i]),
                                                     std::memory_order_relaxed);
            }

            Slot *last = pushed_slots[count - 1U];
            std::uint64_t head = free_head.load(std::memory_order_relaxed);
            for (;;) {
                last->next_free.store(free_head_index(head), std::memory_order_relaxed);
                const std::uint64_t desired = pack_free_head(first, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                    return;
                }
            }
        }

        Block *next{nullptr};
        alignas(hardware_cache_line_size) std::atomic<std::uint64_t> free_head{
            pack_free_head(null_slot_index, 0U)};
        Slot slots[ChunkSize];

    private:
        [[nodiscard]] static constexpr std::uint64_t
        pack_free_head(slot_index_type index, std::uint64_t version) noexcept {
            return (version << slot_index_bits) | static_cast<std::uint64_t>(index);
        }

        [[nodiscard]] static constexpr slot_index_type
        free_head_index(std::uint64_t head) noexcept {
            return static_cast<slot_index_type>(head & slot_index_mask);
        }

        [[nodiscard]] static constexpr std::uint64_t
        free_head_version(std::uint64_t head) noexcept {
            return head >> slot_index_bits;
        }

        [[nodiscard]] slot_index_type slot_index(const Slot *slot) const noexcept {
            const auto index = static_cast<std::size_t>(slot - slots);
            AF_ASSERT(index < ChunkSize);
            return static_cast<slot_index_type>(index);
        }

        [[nodiscard]] slot_index_type released_slot_index(const Slot *slot) const noexcept {
            if constexpr (cache_allocated_slot_index) {
                const slot_index_type index = slot->next_free.load(std::memory_order_relaxed);
                AF_ASSERT(index != null_slot_index);
                return index;
            }
            return slot_index(slot);
        }
    };

    struct LocalCache {
        ObjectPool *owner{nullptr};
        Slot *slots[local_cache_capacity]{};
        std::size_t size{0};
        bool locally_acquired{false};

        ~LocalCache() {
            flush();
        }

        void reset_for(ObjectPool *pool) noexcept {
            if (owner == pool) {
                return;
            }
            flush();
            owner = pool;
        }

        void discard_if_owner(const ObjectPool *pool) noexcept {
            if (owner == pool) {
                size = 0;
                locally_acquired = false;
                owner = nullptr;
            }
        }

        void flush() noexcept {
            flush_some(size);
            locally_acquired = false;
            owner = nullptr;
        }

        void flush_some(std::size_t count) noexcept {
            while (count != 0U && size != 0U) {
                Block *block = slots[size - 1U]->owner;
                const std::size_t end = size;
                do {
                    --size;
                    --count;
                } while (count != 0U && size != 0U && slots[size - 1U]->owner == block);
                block->push_many(slots + size, end - size);
            }
        }

        [[nodiscard]] Slot *pop() noexcept {
            if (size == 0U) [[unlikely]] {
                return nullptr;
            }
            return slots[--size];
        }

        void push(Slot *slot) noexcept {
            slots[size++] = slot;
        }

        void push_remote_release(Slot *slot) noexcept {
            slots[size++] = slot;
            if (size == remote_release_batch_size) {
                flush_some(size);
            }
        }

        [[nodiscard]] bool full() const noexcept {
            return size == local_cache_capacity;
        }

        void mark_locally_acquired() noexcept {
            locally_acquired = true;
        }

        [[nodiscard]] bool caches_releases() const noexcept {
            return locally_acquired;
        }
    };

    struct DirectReleaseSet {
        ObjectPool *owners[direct_release_set_size]{};
        std::size_t next_victim{0};

        [[nodiscard]] bool contains(ObjectPool *pool) const noexcept {
            if (owners[0] == pool) {
                return true;
            }
            for (std::size_t i = 1; i < direct_release_set_size; ++i) {
                if (owners[i] == pool) {
                    return true;
                }
            }
            return false;
        }

        void insert(ObjectPool *pool) noexcept {
            if (contains(pool)) {
                return;
            }
            for (std::size_t i = 0; i < direct_release_set_size; ++i) {
                if (owners[i] == nullptr) {
                    owners[i] = pool;
                    next_victim = i + 1U;
                    if (next_victim == direct_release_set_size) {
                        next_victim = 0;
                    }
                    return;
                }
            }

            owners[next_victim] = pool;
            ++next_victim;
            if (next_victim == direct_release_set_size) {
                next_victim = 0;
            }
        }

        void erase(const ObjectPool *pool) noexcept {
            for (ObjectPool *&owner : owners) {
                if (owner == pool) {
                    owner = nullptr;
                }
            }
        }
    };

    struct SingleLocalCacheSet {
        LocalCache primary{};

        [[nodiscard]] LocalCache &get(ObjectPool *pool) noexcept {
            if (primary.owner == pool) [[likely]] {
                return primary;
            }
            return get_slow(pool);
        }

        [[nodiscard]] LocalCache *find_release_cache(ObjectPool *pool) noexcept {
            if (primary.owner == pool) [[likely]] {
                return &primary;
            }
            if (direct_release_contains(pool)) {
                return nullptr;
            }
            direct_release_insert(pool);
            return nullptr;
        }

        void discard_if_owner(const ObjectPool *pool) noexcept {
            primary.discard_if_owner(pool);
            direct_release_erase(pool);
        }

        [[nodiscard]] AF_DETAIL_NOINLINE LocalCache &get_slow(ObjectPool *pool) noexcept {
            direct_release_erase(pool);
            primary.reset_for(pool);
            return primary;
        }

        [[nodiscard]] static bool direct_release_contains(ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                return direct_release_set().contains(pool);
            }
            static_cast<void>(pool);
            return false;
        }

        static void direct_release_insert(ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                direct_release_set().insert(pool);
            } else {
                static_cast<void>(pool);
            }
        }

        static void direct_release_erase(const ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                direct_release_set().erase(pool);
            } else {
                static_cast<void>(pool);
            }
        }
    };

    struct MultiLocalCacheSet {
        static constexpr std::size_t overflow_cache_count = local_cache_set_size - 1U;

        LocalCache primary{};
        std::array<LocalCache, overflow_cache_count> overflow{};
        LocalCache *active_overflow{overflow.data()};
        std::size_t active_overflow_index{0};
        std::size_t next_victim{0};

        [[nodiscard]] LocalCache &get(ObjectPool *pool) noexcept {
            if (primary.owner == pool) [[likely]] {
                return primary;
            }
            if (active_overflow->owner == pool) {
                return *active_overflow;
            }
            if (LocalCache *hinted = next_overflow_hint(pool)) {
                return *hinted;
            }
            return get_slow(pool);
        }

        [[nodiscard]] LocalCache *find_release_cache(ObjectPool *pool) noexcept {
            if (primary.owner == pool) [[likely]] {
                return &primary;
            }
            if (active_overflow->owner == pool) {
                return active_overflow;
            }
            if (direct_release_contains(pool)) {
                return nullptr;
            }
            if (LocalCache *hinted = next_overflow_hint(pool)) {
                return hinted;
            }
            return find_release_cache_slow(pool);
        }

        void discard_if_owner(const ObjectPool *pool) noexcept {
            primary.discard_if_owner(pool);
            for (LocalCache &entry : overflow) {
                entry.discard_if_owner(pool);
            }
            direct_release_erase(pool);
        }

        [[nodiscard]] AF_DETAIL_NOINLINE LocalCache &get_slow(ObjectPool *pool) noexcept {
            direct_release_erase(pool);

            for (std::size_t i = 0; i < overflow_cache_count; ++i) {
                if (overflow[i].owner == pool) {
                    set_active_overflow(i);
                    return overflow[i];
                }
            }

            if (primary.owner == nullptr) {
                primary.reset_for(pool);
                return primary;
            }

            for (std::size_t i = 0; i < overflow_cache_count; ++i) {
                if (overflow[i].owner == nullptr) {
                    overflow[i].reset_for(pool);
                    set_active_overflow(i);
                    return overflow[i];
                }
            }

            const std::size_t victim = next_victim;
            LocalCache &entry = overflow[victim];
            ++next_victim;
            if (next_victim == overflow_cache_count) {
                next_victim = 0;
            }
            entry.reset_for(pool);
            set_active_overflow(victim);
            return entry;
        }

        [[nodiscard]] AF_DETAIL_NOINLINE LocalCache *
        find_release_cache_slow(ObjectPool *pool) noexcept {
            for (std::size_t i = 0; i < overflow_cache_count; ++i) {
                if (overflow[i].owner == pool) {
                    set_active_overflow(i);
                    return &overflow[i];
                }
            }
            direct_release_insert(pool);
            return nullptr;
        }

        [[nodiscard]] static bool direct_release_contains(ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                return direct_release_set().contains(pool);
            }
            static_cast<void>(pool);
            return false;
        }

        static void direct_release_insert(ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                direct_release_set().insert(pool);
            } else {
                static_cast<void>(pool);
            }
        }

        static void direct_release_erase(const ObjectPool *pool) noexcept {
            if constexpr (remote_release_batch_size == 1U) {
                direct_release_set().erase(pool);
            } else {
                static_cast<void>(pool);
            }
        }

        [[nodiscard]] LocalCache *next_overflow_hint(ObjectPool *pool) noexcept {
            std::size_t next = active_overflow_index + 1U;
            if (next == overflow_cache_count) {
                next = 0;
            }
            if (next != active_overflow_index && overflow[next].owner == pool) {
                set_active_overflow(next);
                return &overflow[next];
            }
            return nullptr;
        }

        void set_active_overflow(std::size_t index) noexcept {
            AF_ASSERT(index < overflow_cache_count);
            active_overflow_index = index;
            active_overflow = &overflow[index];
        }
    };

    using LocalCacheSet =
        std::conditional_t<local_cache_set_size == 1U, SingleLocalCacheSet, MultiLocalCacheSet>;

    [[nodiscard]] void *acquire_slot() {
        LocalCache &cache = local_cache();
        if (Slot *slot = cache.pop()) {
            if (!cache.caches_releases()) [[unlikely]] {
                cache.mark_locally_acquired();
            }
            return slot->storage;
        }
        cache.mark_locally_acquired();
        return acquire_slot_slow(cache);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE void *acquire_slot_slow(LocalCache &cache) {
        for (;;) {
            if (Block *block = hot_block_.load(std::memory_order_acquire)) {
                refill_cache_from_block(cache, *block);
                if (Slot *slot = cache.pop()) {
                    return slot->storage;
                }
            }

            for (Block *block = blocks_.load(std::memory_order_acquire); block != nullptr;
                 block = block->next) {
                refill_cache_from_block(cache, *block);
                if (Slot *slot = cache.pop()) {
                    hot_block_.store(block, std::memory_order_release);
                    return slot->storage;
                }
            }

            Block *block = add_block();
            refill_cache_from_block(cache, *block);
            if (Slot *slot = cache.pop()) {
                return slot->storage;
            }
        }
    }

    void release_slot(void *memory) noexcept {
        Slot *slot = slot_from_memory(memory);
        if constexpr (remote_release_batch_size == 1U) {
            if (LocalCache *cache = tls_caches().find_release_cache(this);
                cache != nullptr && cache->caches_releases()) [[likely]] {
                if (cache->full()) [[unlikely]] {
                    flush_full_cache(*cache);
                }
                cache->push(slot);
            } else {
                slot->owner->push(slot);
            }
        } else {
            LocalCache &cache = local_cache();
            if (!cache.caches_releases()) [[unlikely]] {
                cache.push_remote_release(slot);
                return;
            }
            if (cache.full()) [[unlikely]] {
                flush_full_cache(cache);
            }
            cache.push(slot);
        }
    }

    [[nodiscard]] static Slot *slot_from_memory(void *memory) noexcept {
        auto *bytes = static_cast<std::byte *>(memory);
        return reinterpret_cast<Slot *>(bytes - offsetof(Slot, storage));
    }

    [[nodiscard]] LocalCache &local_cache() noexcept {
        return tls_caches().get(this);
    }

    [[nodiscard]] static LocalCacheSet &tls_caches() noexcept {
        thread_local LocalCacheSet caches;
        return caches;
    }

    [[nodiscard]] static DirectReleaseSet &direct_release_set() noexcept {
        thread_local DirectReleaseSet set;
        return set;
    }

    static void refill_cache_from_block(LocalCache &cache, Block &block) noexcept {
        const std::size_t available = local_cache_capacity - cache.size;
        cache.size += block.try_pop_many(cache.slots + cache.size, available);
    }

    AF_DETAIL_NOINLINE void flush_full_cache(LocalCache &cache) noexcept {
        cache.flush_some(local_cache_flush_count);
    }

    [[nodiscard]] Block *add_block() {
        auto *block = new Block;
        Block *head = blocks_.load(std::memory_order_relaxed);
        do {
            block->next = head;
        } while (!blocks_.compare_exchange_weak(head, block, std::memory_order_release,
                                                std::memory_order_relaxed));
        block_count_.fetch_add(1U, std::memory_order_release);
        hot_block_.store(block, std::memory_order_release);
        return block;
    }

    alignas(hardware_cache_line_size) std::atomic<Block *> blocks_{nullptr};
    alignas(hardware_cache_line_size) std::atomic<Block *> hot_block_{nullptr};
    alignas(hardware_cache_line_size) std::atomic<std::size_t> block_count_{0};
};

} // namespace af::detail
