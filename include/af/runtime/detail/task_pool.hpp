#pragma once

#include <atomic>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/memory/object_pool.hpp"
#include "af/runtime/config_types.hpp"

namespace af::detail {

template <std::size_t LocalCacheCapacity>
inline constexpr std::size_t runtime_task_pool_remote_release_batch_size =
    LocalCacheCapacity < 64U ? LocalCacheCapacity : 64U;

template <typename TaskT, std::size_t LocalCacheCapacity>
using RuntimeTaskPool =
    ObjectPool<TaskT, 4096, runtime_task_pool_remote_release_batch_size<LocalCacheCapacity>, false,
               1, 4, LocalCacheCapacity>;

template <typename TaskT, std::size_t LocalCacheCapacity> struct RuntimeTaskPoolHolder {
    static constexpr std::size_t local_cache_capacity = LocalCacheCapacity;

    RuntimeTaskPool<TaskT, LocalCacheCapacity> pool;
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

template <typename TaskT, std::size_t LocalCacheCapacity>
[[nodiscard]] RuntimeTaskPoolHolder<TaskT, LocalCacheCapacity> &runtime_task_pool_holder() {
    static RuntimeTaskPoolHolder<TaskT, LocalCacheCapacity> holder;
    return holder;
}

template <typename TaskT, std::size_t LocalCacheCapacity>
[[nodiscard]] RuntimeTaskPool<TaskT, LocalCacheCapacity> &runtime_task_pool() {
    return runtime_task_pool_holder<TaskT, LocalCacheCapacity>().pool;
}

template <typename TaskT, std::size_t LocalCacheCapacity>
void destroy_runtime_task(runtime_task *task) noexcept {
    runtime_task_pool<TaskT, LocalCacheCapacity>().destroy(static_cast<TaskT *>(task));
}

template <typename TaskT, typename Fn>
decltype(auto) visit_runtime_task_pool_holder(std::size_t local_cache_size, Fn &&fn) {
    if (local_cache_size > 128U && local_cache_size <= 256U) [[likely]] {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 256U>());
    }
    if (local_cache_size <= 2U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 2U>());
    }
    if (local_cache_size <= 4U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 4U>());
    }
    if (local_cache_size <= 8U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 8U>());
    }
    if (local_cache_size <= 16U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 16U>());
    }
    if (local_cache_size <= 32U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 32U>());
    }
    if (local_cache_size <= 64U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 64U>());
    }
    if (local_cache_size <= 128U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 128U>());
    }
    if (local_cache_size <= 512U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 512U>());
    }
    if (local_cache_size <= 1024U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 1024U>());
    }
    if (local_cache_size <= 2048U) {
        return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 2048U>());
    }
    return std::forward<Fn>(fn)(runtime_task_pool_holder<TaskT, 4096U>());
}

} // namespace af::detail
