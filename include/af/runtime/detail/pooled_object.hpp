#pragma once

#include <atomic>
#include <cstddef>
#include <utility>

#include "af/detail/memory/object_pool.hpp"

namespace af::detail {

template <std::size_t LocalCacheCapacity>
inline constexpr std::size_t runtime_pooled_object_remote_release_batch_size =
    LocalCacheCapacity < 64U ? LocalCacheCapacity : 64U;

template <typename ObjectT, std::size_t LocalCacheCapacity>
using runtime_pooled_object_pool_type =
    object_pool<ObjectT, 4096, runtime_pooled_object_remote_release_batch_size<LocalCacheCapacity>,
                false, 1, 4, LocalCacheCapacity>;

template <typename ObjectT, std::size_t LocalCacheCapacity>
struct runtime_pooled_object_pool_holder_type {
    static constexpr std::size_t local_cache_capacity = LocalCacheCapacity;

    runtime_pooled_object_pool_type<ObjectT, LocalCacheCapacity> pool;
    std::atomic<std::size_t> reserved_slots{0};

    void reserve_at_least(std::size_t slot_count) {
        std::size_t observed = reserved_slots.load(std::memory_order_acquire);
        while (observed < slot_count) {
            pool.reserve_slots(slot_count);
            if (reserved_slots.compare_exchange_weak(
                    observed, slot_count, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }
        }
    }

    [[nodiscard]] bool try_reserve_at_least(std::size_t slot_count) noexcept {
        try {
            reserve_at_least(slot_count);
            return true;
        } catch (...) {
            return false;
        }
    }
};

template <typename ObjectT, std::size_t LocalCacheCapacity>
using RuntimePooledObjectPool = runtime_pooled_object_pool_type<ObjectT, LocalCacheCapacity>;

template <typename ObjectT, std::size_t LocalCacheCapacity>
using RuntimePooledObjectPoolHolder =
    runtime_pooled_object_pool_holder_type<ObjectT, LocalCacheCapacity>;

template <typename ObjectT, std::size_t LocalCacheCapacity>
[[nodiscard]] runtime_pooled_object_pool_holder_type<ObjectT, LocalCacheCapacity> &
runtime_pooled_object_pool_holder() {
    static runtime_pooled_object_pool_holder_type<ObjectT, LocalCacheCapacity> holder;
    return holder;
}

template <typename ObjectT, std::size_t LocalCacheCapacity>
[[nodiscard]] runtime_pooled_object_pool_type<ObjectT, LocalCacheCapacity> &
runtime_pooled_object_pool() {
    return runtime_pooled_object_pool_holder<ObjectT, LocalCacheCapacity>().pool;
}

template <typename ObjectT, std::size_t LocalCacheCapacity>
void destroy_runtime_pooled_object(ObjectT *object) noexcept {
    runtime_pooled_object_pool<ObjectT, LocalCacheCapacity>().destroy(object);
}

template <typename ObjectT, typename Fn>
decltype(auto) visit_runtime_pooled_object_pool_holder(std::size_t local_cache_size, Fn &&fn) {
    if (local_cache_size > 128U && local_cache_size <= 256U) [[likely]] {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 256U>());
    }
    if (local_cache_size <= 2U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 2U>());
    }
    if (local_cache_size <= 4U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 4U>());
    }
    if (local_cache_size <= 8U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 8U>());
    }
    if (local_cache_size <= 16U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 16U>());
    }
    if (local_cache_size <= 32U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 32U>());
    }
    if (local_cache_size <= 64U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 64U>());
    }
    if (local_cache_size <= 128U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 128U>());
    }
    if (local_cache_size <= 512U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 512U>());
    }
    if (local_cache_size <= 1024U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 1024U>());
    }
    if (local_cache_size <= 2048U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 2048U>());
    }
    return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<ObjectT, 4096U>());
}

} // namespace af::detail
