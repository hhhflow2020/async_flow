#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/config.hpp"
#include "af/detail/memory/object_pool_block.hpp"
#include "af/detail/memory/object_pool_local_cache.hpp"

namespace af::detail {

template <typename T, std::size_t ChunkSize = 512, std::size_t RemoteReleaseBatchSize = 1,
          bool CacheAllocatedSlotIndex = false, std::size_t LocalCacheSetSize = 8,
          std::size_t DirectReleaseSetSize = 4, std::size_t LocalCacheCapacity = 64>
class object_pool_core {
    static_assert(ChunkSize > 0, "ObjectPool chunk size must be greater than zero");
    static_assert(RemoteReleaseBatchSize > 0,
                  "ObjectPool remote release batch size must be greater than zero");

public:
    object_pool_core() = default;
    object_pool_core(const object_pool_core &) = delete;
    object_pool_core &operator=(const object_pool_core &) = delete;

    ~object_pool_core() {
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
            return construct(memory, std::forward<Args>(args)...);
        } else {
            try {
                return construct(memory, std::forward<Args>(args)...);
            } catch (...) {
                release_slot(memory);
                throw;
            }
        }
    }

    template <typename OomHandler, typename... Args>
    [[nodiscard]] T *create_with_oom_handler(OomHandler &&on_oom, Args &&...args) {
        void *memory = nullptr;
        try {
            memory = acquire_slot();
        } catch (const std::bad_alloc &) {
            std::forward<OomHandler>(on_oom)();
            throw;
        }

        if constexpr (std::is_nothrow_constructible_v<T, Args...>) {
            return construct(memory, std::forward<Args>(args)...);
        } else {
            try {
                return construct(memory, std::forward<Args>(args)...);
            } catch (...) {
                release_slot(memory);
                throw;
            }
        }
    }

    template <typename... Args> [[nodiscard]] T *try_create(Args &&...args) noexcept {
        void *memory = nullptr;
        try {
            memory = acquire_slot();
            if constexpr (std::is_nothrow_constructible_v<T, Args...>) {
                return construct(memory, std::forward<Args>(args)...);
            } else {
                try {
                    return construct(memory, std::forward<Args>(args)...);
                } catch (...) {
                    release_slot(memory);
                    return nullptr;
                }
            }
        } catch (...) {
            return nullptr;
        }
    }

    template <typename... Args> [[nodiscard]] T *create_uncached(Args &&...args) {
        void *memory = acquire_slot_uncached();
        if constexpr (std::is_nothrow_constructible_v<T, Args...>) {
            return construct(memory, std::forward<Args>(args)...);
        } else {
            try {
                return construct(memory, std::forward<Args>(args)...);
            } catch (...) {
                release_slot_uncached(memory);
                throw;
            }
        }
    }

    template <typename... Args> [[nodiscard]] T *try_create_uncached(Args &&...args) noexcept {
        void *memory = nullptr;
        try {
            memory = acquire_slot_uncached();
            if constexpr (std::is_nothrow_constructible_v<T, Args...>) {
                return construct(memory, std::forward<Args>(args)...);
            } else {
                try {
                    return construct(memory, std::forward<Args>(args)...);
                } catch (...) {
                    release_slot_uncached(memory);
                    return nullptr;
                }
            }
        } catch (...) {
            return nullptr;
        }
    }

    void destroy(T *object) noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::destroy_at(object);
        }
        release_slot(object);
    }

    void destroy_uncached(T *object) noexcept {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::destroy_at(object);
        }
        release_slot_uncached(object);
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
    template <typename... Args> [[nodiscard]] static T *construct(void *memory, Args &&...args) {
        return ::new (memory) T(std::forward<Args>(args)...);
    }

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
    using BlockLayout =
        object_pool_block_layout<T, ChunkSize, CacheAllocatedSlotIndex, LocalCacheCapacity>;
    using Slot = typename BlockLayout::Slot;
    using Block = typename BlockLayout::Block;
    using LocalCache =
        object_pool_local_cache<object_pool_core, Slot, LocalCacheCapacity, RemoteReleaseBatchSize>;
    using LocalCacheSet = object_pool_local_cache_set<object_pool_core, Slot, LocalCacheCapacity,
                                                      RemoteReleaseBatchSize, LocalCacheSetSize,
                                                      DirectReleaseSetSize>;

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

    [[nodiscard]] void *acquire_slot_uncached() {
        for (;;) {
            Slot *slot = nullptr;
            if (Block *block = hot_block_.load(std::memory_order_acquire)) {
                if (block->try_pop_many(&slot, 1U) != 0U) {
                    return slot->storage;
                }
            }

            for (Block *block = blocks_.load(std::memory_order_acquire); block != nullptr;
                 block = block->next) {
                if (block->try_pop_many(&slot, 1U) != 0U) {
                    hot_block_.store(block, std::memory_order_release);
                    return slot->storage;
                }
            }

            Block *block = add_block();
            if (block->try_pop_many(&slot, 1U) != 0U) {
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

    void release_slot_uncached(void *memory) noexcept {
        Slot *slot = slot_from_memory(memory);
        slot->owner->push(slot);
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

template <typename T, std::size_t ChunkSize = 512, std::size_t RemoteReleaseBatchSize = 1,
          bool CacheAllocatedSlotIndex = false, std::size_t LocalCacheSetSize = 8,
          std::size_t DirectReleaseSetSize = 4, std::size_t LocalCacheCapacity = 64>
using ObjectPoolCore =
    object_pool_core<T, ChunkSize, RemoteReleaseBatchSize, CacheAllocatedSlotIndex,
                     LocalCacheSetSize, DirectReleaseSetSize, LocalCacheCapacity>;

} // namespace af::detail
