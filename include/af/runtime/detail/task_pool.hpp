#pragma once

#include <cstddef>
#include <utility>

#include "af/runtime/detail/pooled_object.hpp"

namespace af::detail {

template <typename task_t, std::size_t local_cache_capacity_v>
using runtime_task_pool_type = runtime_pooled_object_pool_type<task_t, local_cache_capacity_v>;

template <typename task_t, std::size_t local_cache_capacity_v>
using runtime_task_pool_holder_type =
    runtime_pooled_object_pool_holder_type<task_t, local_cache_capacity_v>;

template <typename task_t, std::size_t local_cache_capacity_v>
[[nodiscard]] runtime_task_pool_holder_type<task_t, local_cache_capacity_v> &
runtime_task_pool_holder() {
    return runtime_pooled_object_pool_holder<task_t, local_cache_capacity_v>();
}

template <typename task_t, std::size_t local_cache_capacity_v>
[[nodiscard]] runtime_task_pool_type<task_t, local_cache_capacity_v> &runtime_task_pool() {
    return runtime_pooled_object_pool<task_t, local_cache_capacity_v>();
}

template <typename task_t, std::size_t local_cache_capacity_v>
void destroy_runtime_task(runtime_task *task) noexcept {
    destroy_runtime_pooled_object<task_t, local_cache_capacity_v>(static_cast<task_t *>(task));
}

template <typename task_t, typename Fn>
decltype(auto) visit_runtime_task_pool_holder(std::size_t local_cache_size, Fn &&fn) {
    return visit_runtime_pooled_object_pool_holder<task_t>(local_cache_size, std::forward<Fn>(fn));
}

} // namespace af::detail
