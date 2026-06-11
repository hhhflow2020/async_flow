#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename pool_type, typename slot_type, std::size_t local_cache_capacity_value,
          std::size_t remote_release_batch_size_value>
struct object_pool_local_cache {
    pool_type *owner{nullptr};
    std::uint64_t owner_token{0};
    std::weak_ptr<void> owner_lifetime;
    slot_type *slots[local_cache_capacity_value]{};
    std::size_t size{0};
    bool locally_acquired{false};

    ~object_pool_local_cache() {
        flush();
    }

    void reset_for(pool_type *pool) noexcept {
        if (owns(pool)) {
            return;
        }
        if (owner == pool) {
            discard();
        } else {
            flush();
        }
        owner = pool;
        owner_token = pool->cache_token();
        owner_lifetime = pool->cache_lifetime();
    }

    void discard_if_owner(const pool_type *pool) noexcept {
        if (owner == pool) {
            discard();
        }
    }

    void flush() noexcept {
        if (size != 0U) {
            if (auto alive = owner_lifetime.lock(); alive && owner != nullptr) {
                flush_some(size);
            }
        }
        discard();
    }

    void discard() noexcept {
        size = 0;
        locally_acquired = false;
        owner = nullptr;
        owner_token = 0;
        owner_lifetime.reset();
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

    [[nodiscard]] slot_type *pop() noexcept {
        if (size == 0U) [[unlikely]] {
            return nullptr;
        }
        return slots[--size];
    }

    void push(slot_type *slot) noexcept {
        slots[size++] = slot;
    }

    void push_remote_release(slot_type *slot) noexcept {
        slots[size++] = slot;
        if (size == remote_release_batch_size_value) {
            flush_some(size);
        }
    }

    [[nodiscard]] bool full() const noexcept {
        return size == local_cache_capacity_value;
    }

    void mark_locally_acquired() noexcept {
        locally_acquired = true;
    }

    [[nodiscard]] bool caches_releases() const noexcept {
        return locally_acquired;
    }

    [[nodiscard]] bool owns(pool_type *pool) const noexcept {
        return owner == pool && pool != nullptr && owner_token == pool->cache_token();
    }
};

template <typename pool_type, std::size_t direct_release_set_size_value>
struct object_pool_direct_release_set {
    pool_type *owners[direct_release_set_size_value]{};
    std::size_t next_victim{0};

    [[nodiscard]] bool contains(pool_type *pool) const noexcept {
        if (owners[0] == pool) {
            return true;
        }
        for (std::size_t i = 1; i < direct_release_set_size_value; ++i) {
            if (owners[i] == pool) {
                return true;
            }
        }
        return false;
    }

    void insert(pool_type *pool) noexcept {
        if (contains(pool)) {
            return;
        }
        for (std::size_t i = 0; i < direct_release_set_size_value; ++i) {
            if (owners[i] == nullptr) {
                owners[i] = pool;
                next_victim = i + 1U;
                if (next_victim == direct_release_set_size_value) {
                    next_victim = 0;
                }
                return;
            }
        }

        owners[next_victim] = pool;
        ++next_victim;
        if (next_victim == direct_release_set_size_value) {
            next_victim = 0;
        }
    }

    void erase(const pool_type *pool) noexcept {
        for (pool_type *&owner : owners) {
            if (owner == pool) {
                owner = nullptr;
            }
        }
    }
};

template <typename pool_type, typename slot_type, std::size_t local_cache_capacity_value,
          std::size_t remote_release_batch_size_value, std::size_t direct_release_set_size_value>
struct object_pool_single_local_cache_set {
    using local_cache_type =
        object_pool_local_cache<pool_type, slot_type, local_cache_capacity_value,
                                remote_release_batch_size_value>;
    using direct_release_set_type =
        object_pool_direct_release_set<pool_type, direct_release_set_size_value>;

    local_cache_type primary{};

    [[nodiscard]] local_cache_type &get(pool_type *pool) noexcept {
        if (primary.owns(pool)) [[likely]] {
            return primary;
        }
        return get_slow(pool);
    }

    [[nodiscard]] local_cache_type *find_release_cache(pool_type *pool) noexcept {
        if (primary.owns(pool)) [[likely]] {
            return &primary;
        }
        if (direct_release_contains(pool)) {
            return nullptr;
        }
        direct_release_insert(pool);
        return nullptr;
    }

    void discard_if_owner(const pool_type *pool) noexcept {
        primary.discard_if_owner(pool);
        direct_release_erase(pool);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE local_cache_type &get_slow(pool_type *pool) noexcept {
        direct_release_erase(pool);
        primary.reset_for(pool);
        return primary;
    }

    [[nodiscard]] static bool direct_release_contains(pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            return direct_release_set().contains(pool);
        }
        static_cast<void>(pool);
        return false;
    }

    static void direct_release_insert(pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            direct_release_set().insert(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    static void direct_release_erase(const pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            direct_release_set().erase(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    [[nodiscard]] static direct_release_set_type &direct_release_set() noexcept {
        thread_local direct_release_set_type set;
        return set;
    }
};

template <typename pool_type, typename slot_type, std::size_t local_cache_capacity_value,
          std::size_t remote_release_batch_size_value, std::size_t local_cache_set_size_value,
          std::size_t direct_release_set_size_value>
struct object_pool_multi_local_cache_set {
    static constexpr std::size_t overflow_cache_count = local_cache_set_size_value - 1U;

    using local_cache_type =
        object_pool_local_cache<pool_type, slot_type, local_cache_capacity_value,
                                remote_release_batch_size_value>;
    using direct_release_set_type =
        object_pool_direct_release_set<pool_type, direct_release_set_size_value>;

    local_cache_type primary{};
    std::array<local_cache_type, overflow_cache_count> overflow{};
    local_cache_type *active_overflow{overflow.data()};
    std::size_t active_overflow_index{0};
    std::size_t next_victim{0};

    [[nodiscard]] local_cache_type &get(pool_type *pool) noexcept {
        if (primary.owns(pool)) [[likely]] {
            return primary;
        }
        if (active_overflow->owns(pool)) {
            return *active_overflow;
        }
        if (local_cache_type *hinted = next_overflow_hint(pool)) {
            return *hinted;
        }
        return get_slow(pool);
    }

    [[nodiscard]] local_cache_type *find_release_cache(pool_type *pool) noexcept {
        if (primary.owns(pool)) [[likely]] {
            return &primary;
        }
        if (active_overflow->owns(pool)) {
            return active_overflow;
        }
        if (direct_release_contains(pool)) {
            return nullptr;
        }
        if (local_cache_type *hinted = next_overflow_hint(pool)) {
            return hinted;
        }
        return find_release_cache_slow(pool);
    }

    void discard_if_owner(const pool_type *pool) noexcept {
        primary.discard_if_owner(pool);
        for (local_cache_type &entry : overflow) {
            entry.discard_if_owner(pool);
        }
        direct_release_erase(pool);
    }

    [[nodiscard]] AF_DETAIL_NOINLINE local_cache_type &get_slow(pool_type *pool) noexcept {
        direct_release_erase(pool);

        if (primary.owner == pool) {
            primary.reset_for(pool);
            return primary;
        }

        for (std::size_t i = 0; i < overflow_cache_count; ++i) {
            if (overflow[i].owns(pool) || overflow[i].owner == pool) {
                overflow[i].reset_for(pool);
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
        local_cache_type &entry = overflow[victim];
        ++next_victim;
        if (next_victim == overflow_cache_count) {
            next_victim = 0;
        }
        entry.reset_for(pool);
        set_active_overflow(victim);
        return entry;
    }

    [[nodiscard]] AF_DETAIL_NOINLINE local_cache_type *
    find_release_cache_slow(pool_type *pool) noexcept {
        if (primary.owner == pool) {
            primary.reset_for(pool);
            return &primary;
        }
        for (std::size_t i = 0; i < overflow_cache_count; ++i) {
            if (overflow[i].owns(pool) || overflow[i].owner == pool) {
                overflow[i].reset_for(pool);
                set_active_overflow(i);
                return &overflow[i];
            }
        }
        direct_release_insert(pool);
        return nullptr;
    }

    [[nodiscard]] static bool direct_release_contains(pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            return direct_release_set().contains(pool);
        }
        static_cast<void>(pool);
        return false;
    }

    static void direct_release_insert(pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            direct_release_set().insert(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    static void direct_release_erase(const pool_type *pool) noexcept {
        if constexpr (remote_release_batch_size_value == 1U) {
            direct_release_set().erase(pool);
        } else {
            static_cast<void>(pool);
        }
    }

    [[nodiscard]] local_cache_type *next_overflow_hint(pool_type *pool) noexcept {
        std::size_t next = active_overflow_index + 1U;
        if (next == overflow_cache_count) {
            next = 0;
        }
        if (next != active_overflow_index && overflow[next].owns(pool)) {
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

    [[nodiscard]] static direct_release_set_type &direct_release_set() noexcept {
        thread_local direct_release_set_type set;
        return set;
    }
};

template <typename pool_type, typename slot_type, std::size_t local_cache_capacity_value,
          std::size_t remote_release_batch_size_value, std::size_t local_cache_set_size_value,
          std::size_t direct_release_set_size_value>
using object_pool_local_cache_set = std::conditional_t<
    local_cache_set_size_value == 1U,
    object_pool_single_local_cache_set<pool_type, slot_type, local_cache_capacity_value,
                                       remote_release_batch_size_value,
                                       direct_release_set_size_value>,
    object_pool_multi_local_cache_set<pool_type, slot_type, local_cache_capacity_value,
                                      remote_release_batch_size_value, local_cache_set_size_value,
                                      direct_release_set_size_value>>;

} // namespace af::detail
