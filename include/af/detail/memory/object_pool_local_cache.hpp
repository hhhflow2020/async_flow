#pragma once

#include <array>
#include <cstddef>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize>
struct object_pool_local_cache {
    Pool *owner{nullptr};
    Slot *slots[LocalCacheCapacity]{};
    std::size_t size{0};
    bool locally_acquired{false};

    ~object_pool_local_cache() {
        flush();
    }

    void reset_for(Pool *pool) noexcept {
        if (owner == pool) {
            return;
        }
        flush();
        owner = pool;
    }

    void discard_if_owner(const Pool *pool) noexcept {
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
            auto *block = slots[size - 1U]->owner;
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
        if (size == RemoteReleaseBatchSize) {
            flush_some(size);
        }
    }

    [[nodiscard]] bool full() const noexcept {
        return size == LocalCacheCapacity;
    }

    void mark_locally_acquired() noexcept {
        locally_acquired = true;
    }

    [[nodiscard]] bool caches_releases() const noexcept {
        return locally_acquired;
    }
};

template <typename Pool, std::size_t DirectReleaseSetSize> struct object_pool_direct_release_set {
    Pool *owners[DirectReleaseSetSize]{};
    std::size_t next_victim{0};

    [[nodiscard]] bool contains(Pool *pool) const noexcept {
        if (owners[0] == pool) {
            return true;
        }
        for (std::size_t i = 1; i < DirectReleaseSetSize; ++i) {
            if (owners[i] == pool) {
                return true;
            }
        }
        return false;
    }

    void insert(Pool *pool) noexcept {
        if (contains(pool)) {
            return;
        }
        for (std::size_t i = 0; i < DirectReleaseSetSize; ++i) {
            if (owners[i] == nullptr) {
                owners[i] = pool;
                next_victim = i + 1U;
                if (next_victim == DirectReleaseSetSize) {
                    next_victim = 0;
                }
                return;
            }
        }

        owners[next_victim] = pool;
        ++next_victim;
        if (next_victim == DirectReleaseSetSize) {
            next_victim = 0;
        }
    }

    void erase(const Pool *pool) noexcept {
        for (Pool *&owner : owners) {
            if (owner == pool) {
                owner = nullptr;
            }
        }
    }
};

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t DirectReleaseSetSize>
struct object_pool_single_local_cache_set {
    using LocalCache =
        object_pool_local_cache<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize>;
    using DirectReleaseSet = object_pool_direct_release_set<Pool, DirectReleaseSetSize>;

    LocalCache primary{};

    [[nodiscard]] LocalCache &get(Pool *pool) noexcept {
        if (primary.owner == pool) [[likely]] {
            return primary;
        }
        return get_slow(pool);
    }

    [[nodiscard]] LocalCache *find_release_cache(Pool *pool) noexcept {
        if (primary.owner == pool) [[likely]] {
            return &primary;
        }
        if (direct_release_contains(pool)) {
            return nullptr;
        }
        direct_release_insert(pool);
        return nullptr;
    }

    void discard_if_owner(const Pool *pool) noexcept {
        primary.discard_if_owner(pool);
        direct_release_erase(pool);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE LocalCache &get_slow(Pool *pool) noexcept {
        direct_release_erase(pool);
        primary.reset_for(pool);
        return primary;
    }

    [[nodiscard]] static bool direct_release_contains(Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            return direct_release_set().contains(pool);
        }
        static_cast<void>(pool);
        return false;
    }

    static void direct_release_insert(Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            direct_release_set().insert(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    static void direct_release_erase(const Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            direct_release_set().erase(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    [[nodiscard]] static DirectReleaseSet &direct_release_set() noexcept {
        thread_local DirectReleaseSet set;
        return set;
    }
};

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t LocalCacheSetSize,
          std::size_t DirectReleaseSetSize>
struct object_pool_multi_local_cache_set {
    static constexpr std::size_t overflow_cache_count = LocalCacheSetSize - 1U;

    using LocalCache =
        object_pool_local_cache<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize>;
    using DirectReleaseSet = object_pool_direct_release_set<Pool, DirectReleaseSetSize>;

    LocalCache primary{};
    std::array<LocalCache, overflow_cache_count> overflow{};
    LocalCache *active_overflow{overflow.data()};
    std::size_t active_overflow_index{0};
    std::size_t next_victim{0};

    [[nodiscard]] LocalCache &get(Pool *pool) noexcept {
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

    [[nodiscard]] LocalCache *find_release_cache(Pool *pool) noexcept {
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

    void discard_if_owner(const Pool *pool) noexcept {
        primary.discard_if_owner(pool);
        for (LocalCache &entry : overflow) {
            entry.discard_if_owner(pool);
        }
        direct_release_erase(pool);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE LocalCache &get_slow(Pool *pool) noexcept {
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

    [[nodiscard]] AF_DETAIL_NOINLINE LocalCache *find_release_cache_slow(Pool *pool) noexcept {
        for (std::size_t i = 0; i < overflow_cache_count; ++i) {
            if (overflow[i].owner == pool) {
                set_active_overflow(i);
                return &overflow[i];
            }
        }
        direct_release_insert(pool);
        return nullptr;
    }

    [[nodiscard]] static bool direct_release_contains(Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            return direct_release_set().contains(pool);
        }
        static_cast<void>(pool);
        return false;
    }

    static void direct_release_insert(Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            direct_release_set().insert(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    static void direct_release_erase(const Pool *pool) noexcept {
        if constexpr (RemoteReleaseBatchSize == 1U) {
            direct_release_set().erase(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    [[nodiscard]] LocalCache *next_overflow_hint(Pool *pool) noexcept {
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

    [[nodiscard]] static DirectReleaseSet &direct_release_set() noexcept {
        thread_local DirectReleaseSet set;
        return set;
    }
};

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t LocalCacheSetSize,
          std::size_t DirectReleaseSetSize>
using object_pool_local_cache_set = std::conditional_t<
    LocalCacheSetSize == 1U,
    object_pool_single_local_cache_set<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize,
                                       DirectReleaseSetSize>,
    object_pool_multi_local_cache_set<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize,
                                      LocalCacheSetSize, DirectReleaseSetSize>>;

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize>
using ObjectPoolLocalCache =
    object_pool_local_cache<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize>;

template <typename Pool, std::size_t DirectReleaseSetSize>
using ObjectPoolDirectReleaseSet = object_pool_direct_release_set<Pool, DirectReleaseSetSize>;

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t DirectReleaseSetSize>
using ObjectPoolSingleLocalCacheSet =
    object_pool_single_local_cache_set<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize,
                                       DirectReleaseSetSize>;

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t LocalCacheSetSize,
          std::size_t DirectReleaseSetSize>
using ObjectPoolMultiLocalCacheSet =
    object_pool_multi_local_cache_set<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize,
                                      LocalCacheSetSize, DirectReleaseSetSize>;

template <typename Pool, typename Slot, std::size_t LocalCacheCapacity,
          std::size_t RemoteReleaseBatchSize, std::size_t LocalCacheSetSize,
          std::size_t DirectReleaseSetSize>
using ObjectPoolLocalCacheSet =
    object_pool_local_cache_set<Pool, Slot, LocalCacheCapacity, RemoteReleaseBatchSize,
                                LocalCacheSetSize, DirectReleaseSetSize>;

} // namespace af::detail
