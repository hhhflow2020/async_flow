#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"

namespace af::detail {

template <typename T, std::size_t ChunkSize = 256>
class ObjectPool {
    static_assert(ChunkSize > 0, "ObjectPool chunk size must be greater than zero");

public:
    ObjectPool() = default;
    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;

private:
#define AF_OBJECT_POOL_FRAGMENT_INCLUDE 1
#include "af/detail/object_pool_storage_fragment.hpp"
#include "af/detail/object_pool_slot_ops_fragment.hpp"
#undef AF_OBJECT_POOL_FRAGMENT_INCLUDE

public:
#define AF_OBJECT_POOL_FRAGMENT_INCLUDE 1
#include "af/detail/object_pool_lifecycle_fragment.hpp"
#undef AF_OBJECT_POOL_FRAGMENT_INCLUDE
};

} // namespace af::detail
