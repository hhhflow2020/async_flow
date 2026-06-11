#pragma once

#include <cstddef>

#include "af/detail/memory/object_pool_core.hpp"

namespace af::detail {

template <typename T, std::size_t chunk_size_v = 512, std::size_t remote_release_batch_size_v = 1,
          bool cache_allocated_slot_index_v = false, std::size_t local_cache_set_size_v = 8,
          std::size_t direct_release_set_size_v = 4, std::size_t local_cache_capacity_v = 64>
using object_pool =
    object_pool_core<T, chunk_size_v, remote_release_batch_size_v, cache_allocated_slot_index_v,
                     local_cache_set_size_v, direct_release_set_size_v, local_cache_capacity_v>;

} // namespace af::detail
