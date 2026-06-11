#pragma once

#include <cstddef>
#include <utility>

#include "af/detail/memory/object_pool.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"

namespace af::detail {

template <std::size_t local_cache_capacity_v>
inline constexpr std::size_t runtime_pooled_object_remote_release_batch_size =
    local_cache_capacity_v < 64U ? local_cache_capacity_v : 64U;

template <typename object_t, std::size_t local_cache_capacity_v>
using runtime_pooled_object_pool_type =
    object_pool<object_t, 4096,
                runtime_pooled_object_remote_release_batch_size<local_cache_capacity_v>, false, 1,
                4, local_cache_capacity_v>;

template <typename object_t, std::size_t local_cache_capacity_v>
struct runtime_pooled_object_pool_holder_type {
    static constexpr std::size_t local_cache_capacity = local_cache_capacity_v;

    runtime_pooled_object_pool_type<object_t, local_cache_capacity_v> pool;
    cache_line_atomic<std::size_t> reserved_slots{0};

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

template <typename object_t, std::size_t local_cache_capacity_v>
[[nodiscard]] runtime_pooled_object_pool_holder_type<object_t, local_cache_capacity_v> &
runtime_pooled_object_pool_holder() {
    static runtime_pooled_object_pool_holder_type<object_t, local_cache_capacity_v> holder;
    return holder;
}

template <typename object_t, std::size_t local_cache_capacity_v>
[[nodiscard]] runtime_pooled_object_pool_type<object_t, local_cache_capacity_v> &
runtime_pooled_object_pool() {
    return runtime_pooled_object_pool_holder<object_t, local_cache_capacity_v>().pool;
}

template <typename object_t, std::size_t local_cache_capacity_v>
void destroy_runtime_pooled_object(object_t *object) noexcept {
    runtime_pooled_object_pool<object_t, local_cache_capacity_v>().destroy(object);
}

template <typename object_t, typename Fn>
decltype(auto) visit_runtime_pooled_object_pool_holder(std::size_t local_cache_size, Fn &&fn) {
    if (local_cache_size > 128U && local_cache_size <= 256U) [[likely]] {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 256U>());
    }
    if (local_cache_size <= 2U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 2U>());
    }
    if (local_cache_size <= 4U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 4U>());
    }
    if (local_cache_size <= 8U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 8U>());
    }
    if (local_cache_size <= 16U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 16U>());
    }
    if (local_cache_size <= 32U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 32U>());
    }
    if (local_cache_size <= 64U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 64U>());
    }
    if (local_cache_size <= 128U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 128U>());
    }
    if (local_cache_size <= 512U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 512U>());
    }
    if (local_cache_size <= 1024U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 1024U>());
    }
    if (local_cache_size <= 2048U) {
        return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 2048U>());
    }
    return std::forward<Fn>(fn)(runtime_pooled_object_pool_holder<object_t, 4096U>());
}

} // namespace af::detail
