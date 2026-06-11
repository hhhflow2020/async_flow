#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "af/detail/config.hpp"
#include "af/detail/log/async_log_record_pool.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/memory/contiguous_object_storage.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"

namespace af::detail {

struct alignas(hardware_cache_line_size) async_log_stat_counter {
    void add(std::uint64_t value) noexcept {
        count.fetch_add(value, std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t
    load(std::memory_order order = std::memory_order_relaxed) const noexcept {
        return count.load(order);
    }

    std::atomic<std::uint64_t> count{0};
};

struct alignas(hardware_cache_line_size) async_log_queue_shard {
    async_log_queue_shard(std::size_t queue_capacity, std::size_t record_capacity,
                          std::size_t record_local_cache_size)
        : queue(queue_capacity), records(record_capacity, record_local_cache_size) {}

    async_log_stat_counter accepted;
    async_log_stat_counter dropped;
    BoundedMpscQueue<log_record> queue;
    async_log_record_pool records;
};

struct alignas(hardware_cache_line_size) async_log_ordered_queue {
    explicit async_log_ordered_queue(std::size_t queue_capacity) : queue(queue_capacity) {}

    BoundedMpscQueue<log_record> queue;
};

struct alignas(hardware_cache_line_size) async_log_producer_shard {
    async_log_producer_shard(std::size_t record_capacity, std::size_t record_local_cache_size)
        : records(record_capacity, record_local_cache_size) {}

    async_log_stat_counter accepted;
    async_log_stat_counter dropped;
    async_log_record_pool records;
};

struct alignas(hardware_cache_line_size) async_log_runtime_lane {
    async_log_runtime_lane(std::size_t queue_capacity, std::size_t record_capacity,
                           std::size_t record_local_cache_size)
        : queue(queue_capacity), records(record_capacity, record_local_cache_size) {}

    async_log_stat_counter accepted;
    async_log_stat_counter dropped;
    BoundedMpscQueue<log_record> queue;
    async_log_record_pool records;
};

using async_log_queue_shard_storage = ContiguousObjectStorage<async_log_queue_shard>;
using async_log_producer_shard_storage = ContiguousObjectStorage<async_log_producer_shard>;
using async_log_runtime_lane_storage = ContiguousObjectStorage<async_log_runtime_lane>;

using AsyncLogStatCounter = async_log_stat_counter;
using AsyncLogQueueShard = async_log_queue_shard;
using AsyncLogOrderedQueue = async_log_ordered_queue;
using AsyncLogProducerShard = async_log_producer_shard;
using AsyncLogRuntimeLane = async_log_runtime_lane;
using AsyncLogQueueShardStorage = async_log_queue_shard_storage;
using AsyncLogProducerShardStorage = async_log_producer_shard_storage;
using AsyncLogRuntimeLaneStorage = async_log_runtime_lane_storage;

} // namespace af::detail
