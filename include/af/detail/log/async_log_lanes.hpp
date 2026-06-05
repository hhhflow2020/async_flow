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

struct alignas(hardware_cache_line_size) AsyncLogStatCounter {
    void add(std::uint64_t value) noexcept {
        count.fetch_add(value, std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t
    load(std::memory_order order = std::memory_order_relaxed) const noexcept {
        return count.load(order);
    }

    std::atomic<std::uint64_t> count{0};
};

struct alignas(hardware_cache_line_size) AsyncLogQueueShard {
    AsyncLogQueueShard(std::size_t queue_capacity, std::size_t record_capacity,
                       std::size_t record_local_cache_size)
        : queue(queue_capacity), records(record_capacity, record_local_cache_size) {}

    AsyncLogStatCounter accepted;
    AsyncLogStatCounter dropped;
    BoundedMpscQueue<LogRecord> queue;
    AsyncLogRecordPool records;
};

struct alignas(hardware_cache_line_size) AsyncLogOrderedQueue {
    explicit AsyncLogOrderedQueue(std::size_t queue_capacity) : queue(queue_capacity) {}

    BoundedMpscQueue<LogRecord> queue;
};

struct alignas(hardware_cache_line_size) AsyncLogProducerShard {
    AsyncLogProducerShard(std::size_t record_capacity, std::size_t record_local_cache_size)
        : records(record_capacity, record_local_cache_size) {}

    AsyncLogStatCounter accepted;
    AsyncLogStatCounter dropped;
    AsyncLogRecordPool records;
};

struct alignas(hardware_cache_line_size) AsyncLogRuntimeLane {
    AsyncLogRuntimeLane(std::size_t queue_capacity, std::size_t record_capacity,
                        std::size_t record_local_cache_size)
        : queue(queue_capacity), records(record_capacity, record_local_cache_size) {}

    AsyncLogStatCounter accepted;
    AsyncLogStatCounter dropped;
    BoundedMpscQueue<LogRecord> queue;
    AsyncLogRecordPool records;
};

using AsyncLogQueueShardStorage = ContiguousObjectStorage<AsyncLogQueueShard>;
using AsyncLogProducerShardStorage = ContiguousObjectStorage<AsyncLogProducerShard>;
using AsyncLogRuntimeLaneStorage = ContiguousObjectStorage<AsyncLogRuntimeLane>;

} // namespace af::detail
