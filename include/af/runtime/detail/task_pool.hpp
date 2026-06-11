#pragma once

#include <cstddef>
#include <utility>

#include "af/runtime/detail/pooled_object.hpp"

namespace af::detail {

template <typename TaskT, std::size_t LocalCacheCapacity>
using runtime_task_pool_type = runtime_pooled_object_pool_type<TaskT, LocalCacheCapacity>;

template <typename TaskT, std::size_t LocalCacheCapacity>
using runtime_task_pool_holder_type =
    runtime_pooled_object_pool_holder_type<TaskT, LocalCacheCapacity>;

template <typename TaskT, std::size_t LocalCacheCapacity>
using RuntimeTaskPool = runtime_task_pool_type<TaskT, LocalCacheCapacity>;

template <typename TaskT, std::size_t LocalCacheCapacity>
using RuntimeTaskPoolHolder = runtime_task_pool_holder_type<TaskT, LocalCacheCapacity>;

template <typename TaskT, std::size_t LocalCacheCapacity>
[[nodiscard]] runtime_task_pool_holder_type<TaskT, LocalCacheCapacity> &runtime_task_pool_holder() {
    return runtime_pooled_object_pool_holder<TaskT, LocalCacheCapacity>();
}

template <typename TaskT, std::size_t LocalCacheCapacity>
[[nodiscard]] runtime_task_pool_type<TaskT, LocalCacheCapacity> &runtime_task_pool() {
    return runtime_pooled_object_pool<TaskT, LocalCacheCapacity>();
}

template <typename TaskT, std::size_t LocalCacheCapacity>
void destroy_runtime_task(runtime_task *task) noexcept {
    destroy_runtime_pooled_object<TaskT, LocalCacheCapacity>(static_cast<TaskT *>(task));
}

template <typename TaskT, typename Fn>
decltype(auto) visit_runtime_task_pool_holder(std::size_t local_cache_size, Fn &&fn) {
    return visit_runtime_pooled_object_pool_holder<TaskT>(local_cache_size, std::forward<Fn>(fn));
}

} // namespace af::detail
