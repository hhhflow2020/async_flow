#pragma once

#include <cstddef>

#include "af/detail/memory/object_pool_core.hpp"

namespace af::detail {

template <typename T, std::size_t ChunkSize = 512, std::size_t RemoteReleaseBatchSize = 1,
          bool CacheAllocatedSlotIndex = false, std::size_t LocalCacheSetSize = 8,
          std::size_t DirectReleaseSetSize = 4, std::size_t LocalCacheCapacity = 64>
using object_pool = object_pool_core<T, ChunkSize, RemoteReleaseBatchSize, CacheAllocatedSlotIndex,
                                     LocalCacheSetSize, DirectReleaseSetSize, LocalCacheCapacity>;

} // namespace af::detail
