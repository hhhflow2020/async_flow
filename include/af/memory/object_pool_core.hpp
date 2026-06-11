#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/config.hpp"
#include "af/memory/cache_line.hpp"
#include "af/memory/object_pool_block.hpp"
#include "af/memory/object_pool_local_cache.hpp"

namespace af::detail {

template <typename T, std::size_t chunk_size_v = 512, std::size_t remote_release_batch_size_v = 1,
          bool cache_allocated_slot_index_v = false, std::size_t local_cache_set_size_v = 8,
          std::size_t direct_release_set_size_v = 4, std::size_t local_cache_capacity_v = 64>
class object_pool_core {
    static_assert(chunk_size_v > 0, "ObjectPool chunk size must be greater than zero");
    static_assert(remote_release_batch_size_v > 0,
                  "ObjectPool remote release batch size must be greater than zero");

public:
    object_pool_core() = default;
    object_pool_core(const object_pool_core &) = delete;
    object_pool_core &operator=(const object_pool_core &) = delete;

    ~object_pool_core() {
        lifetime_.reset();
        tls_caches().discard_if_owner(this);
        block_type *block = blocks_.load(std::memory_order_relaxed);
        while (block != nullptr) {
            block_type *next = block->next;
            delete block;
            block = next;
        }
    }

    [[nodiscard]] std::uint64_t cache_token() const noexcept {
        return cache_token_;
    }

    [[nodiscard]] const std::shared_ptr<void> &cache_lifetime() const noexcept {
        return lifetime_;
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
        reserve_blocks(slot_count == 0U ? 0U : ((slot_count - 1U) / chunk_size_v) + 1U);
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

    static constexpr std::size_t local_cache_capacity = local_cache_capacity_v;
    static constexpr std::size_t local_cache_flush_count = local_cache_capacity / 2;
    static constexpr std::size_t remote_release_batch_size = remote_release_batch_size_v;
    static constexpr bool cache_allocated_slot_index = cache_allocated_slot_index_v;
    static constexpr std::size_t local_cache_set_size = local_cache_set_size_v;
    static constexpr std::size_t direct_release_set_size = direct_release_set_size_v;
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
    using block_layout = object_pool_block_layout<T, chunk_size_v, cache_allocated_slot_index_v,
                                                  local_cache_capacity_v>;
    using slot_type = typename block_layout::slot;
    using block_type = typename block_layout::block;
    using local_cache_type =
        object_pool_local_cache<object_pool_core, slot_type, local_cache_capacity_v,
                                remote_release_batch_size_v>;
    using local_cache_set_type =
        object_pool_local_cache_set<object_pool_core, slot_type, local_cache_capacity_v,
                                    remote_release_batch_size_v, local_cache_set_size_v,
                                    direct_release_set_size_v>;

    [[nodiscard]] void *acquire_slot() {
        local_cache_type &cache = local_cache();
        if (slot_type *slot = cache.pop()) {
            if (!cache.caches_releases()) [[unlikely]] {
                cache.mark_locally_acquired();
            }
            return slot->storage;
        }
        cache.mark_locally_acquired();
        return acquire_slot_slow(cache);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE void *acquire_slot_slow(local_cache_type &cache) {
        for (;;) {
            if (block_type *block = hot_block_.load(std::memory_order_acquire)) {
                refill_cache_from_block(cache, *block);
                if (slot_type *slot = cache.pop()) {
                    return slot->storage;
                }
            }

            for (block_type *block = blocks_.load(std::memory_order_acquire); block != nullptr;
                 block = block->next) {
                refill_cache_from_block(cache, *block);
                if (slot_type *slot = cache.pop()) {
                    hot_block_.store(block, std::memory_order_release);
                    return slot->storage;
                }
            }

            block_type *block = add_block();
            refill_cache_from_block(cache, *block);
            if (slot_type *slot = cache.pop()) {
                return slot->storage;
            }
        }
    }

    [[nodiscard]] void *acquire_slot_uncached() {
        for (;;) {
            slot_type *slot = nullptr;
            if (block_type *block = hot_block_.load(std::memory_order_acquire)) {
                if (block->try_pop_many(&slot, 1U) != 0U) {
                    return slot->storage;
                }
            }

            for (block_type *block = blocks_.load(std::memory_order_acquire); block != nullptr;
                 block = block->next) {
                if (block->try_pop_many(&slot, 1U) != 0U) {
                    hot_block_.store(block, std::memory_order_release);
                    return slot->storage;
                }
            }

            block_type *block = add_block();
            if (block->try_pop_many(&slot, 1U) != 0U) {
                return slot->storage;
            }
        }
    }

    void release_slot(void *memory) noexcept {
        slot_type *slot = slot_from_memory(memory);
        if constexpr (remote_release_batch_size == 1U) {
            if (local_cache_type *cache = tls_caches().find_release_cache(this);
                cache != nullptr && cache->caches_releases()) [[likely]] {
                if (cache->full()) [[unlikely]] {
                    flush_full_cache(*cache);
                }
                cache->push(slot);
            } else {
                slot->owner->push(slot);
            }
        } else {
            local_cache_type &cache = local_cache();
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
        slot_type *slot = slot_from_memory(memory);
        slot->owner->push(slot);
    }

    [[nodiscard]] static slot_type *slot_from_memory(void *memory) noexcept {
        auto *bytes = static_cast<std::byte *>(memory);
        return reinterpret_cast<slot_type *>(bytes - offsetof(slot_type, storage));
    }

    [[nodiscard]] local_cache_type &local_cache() noexcept {
        return tls_caches().get(this);
    }

    [[nodiscard]] static local_cache_set_type &tls_caches() noexcept {
        thread_local local_cache_set_type caches;
        return caches;
    }

    static void refill_cache_from_block(local_cache_type &cache, block_type &block) noexcept {
        const std::size_t available = local_cache_capacity - cache.size;
        cache.size += block.try_pop_many(cache.slots + cache.size, available);
    }

    AF_DETAIL_NOINLINE void flush_full_cache(local_cache_type &cache) noexcept {
        cache.flush_some(local_cache_flush_count);
    }

    [[nodiscard]] block_type *add_block() {
        auto *block = new block_type;
        block_type *head = blocks_.load(std::memory_order_relaxed);
        do {
            block->next = head;
        } while (!blocks_.compare_exchange_weak(head, block, std::memory_order_release,
                                                std::memory_order_relaxed));
        block_count_.fetch_add(1U, std::memory_order_release);
        hot_block_.store(block, std::memory_order_release);
        return block;
    }

    inline static std::atomic<std::uint64_t> next_cache_token_{1U};
    const std::uint64_t cache_token_{next_cache_token_.fetch_add(1U, std::memory_order_relaxed)};
    std::shared_ptr<void> lifetime_{std::make_shared<std::uint8_t>(0)};
    alignas(hardware_cache_line_size) std::atomic<block_type *> blocks_{nullptr};
    alignas(hardware_cache_line_size) std::atomic<block_type *> hot_block_{nullptr};
    alignas(hardware_cache_line_size) std::atomic<std::size_t> block_count_{0};
};

} // namespace af::detail
