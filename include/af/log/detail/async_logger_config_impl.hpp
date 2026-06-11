#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>

namespace af {

inline async_logger::async_logger(async_log_config config)
    : cache_token_(next_cache_token_.fetch_add(1U, std::memory_order_relaxed)),
      ordering_(validate_ordering(config.ordering)),
      queue_shard_count_(
          relaxed_queue_shard_count_for_ordering(ordering_, config.queue_shard_count)),
      queue_shard_mask_(queue_shard_count_ == 0U ? 0U : queue_shard_count_ - 1U),
      ordered_producer_shard_count_(
          ordered_producer_shard_count_for_ordering(ordering_, config.queue_shard_count)),
      ordered_producer_shard_mask_(
          ordered_producer_shard_count_ == 0U ? 0U : ordered_producer_shard_count_ - 1U),
      runtime_thread_count_(
          runtime_thread_count_for_ordering(ordering_, config.runtime_thread_count)),
      ordered_queue_(make_ordered_queue(ordering_, config.queue_capacity)),
      ordered_producer_shards_(make_ordered_producer_shards(
          ordered_producer_shard_count_,
          ordered_record_capacity_per_producer_shard(
              config.queue_capacity, ordered_producer_shard_count_, config.max_batch_size),
          config.record_pool_slab_object_count, config.record_pool_local_cache_size)),
      queue_shards_(make_queue_shards(
          queue_shard_count_,
          queue_shard_count_ == 0U
              ? 0U
              : queue_capacity_per_shard(config.queue_capacity, queue_shard_count_),
          config.max_batch_size, config.record_pool_slab_object_count,
          config.record_pool_local_cache_size)),
      runtime_lanes_(
          make_runtime_lanes(runtime_thread_count_,
                             queue_capacity_per_runtime_thread(config.runtime_lane_capacity == 0U
                                                                   ? config.queue_capacity
                                                                   : config.runtime_lane_capacity,
                                                               runtime_thread_count_),
                             config.max_batch_size, config.record_pool_slab_object_count,
                             config.record_pool_local_cache_size)),
      max_batch_size_(config.max_batch_size == 0U ? 1U : config.max_batch_size),
      overflow_spin_count_(config.overflow_spin_count), overflow_policy_(config.overflow_policy),
      flush_poll_interval_(config.flush_poll_interval),
      fatal_flush_timeout_(config.fatal_flush_timeout), backends_(std::move(config.backends)) {}

inline async_log_stats async_logger::stats() const noexcept {
    async_log_stats result;
    for (const detail::async_log_producer_shard &shard : ordered_producer_shards_) {
        result.accepted += shard.accepted.load();
        result.dropped += shard.dropped.load();
    }
    for (const detail::async_log_queue_shard &shard : queue_shards_) {
        result.accepted += shard.accepted.load();
        result.dropped += shard.dropped.load();
    }
    for (const detail::async_log_runtime_lane &lane : runtime_lanes_) {
        result.accepted += lane.accepted.load();
        result.dropped += lane.dropped.load();
    }
    return result;
}

inline std::chrono::milliseconds async_logger::fatal_flush_timeout() const noexcept {
    return fatal_flush_timeout_;
}

inline log_ordering async_logger::validate_ordering(log_ordering ordering) {
    switch (ordering) {
    case log_ordering::ordered:
    case log_ordering::relaxed:
        return ordering;
    }
    throw std::invalid_argument("invalid async log ordering");
}

inline std::size_t async_logger::relaxed_queue_shard_count_for_ordering(log_ordering ordering,
                                                                        std::size_t requested) {
    if (ordering == log_ordering::ordered) {
        return 0U;
    }
    return normalize_queue_shard_count(requested);
}

inline std::size_t async_logger::ordered_producer_shard_count_for_ordering(log_ordering ordering,
                                                                           std::size_t requested) {
    if (ordering == log_ordering::relaxed) {
        return 0U;
    }
    return normalize_queue_shard_count(requested);
}

inline std::size_t async_logger::runtime_thread_count_for_ordering(log_ordering ordering,
                                                                   std::size_t requested) {
    const std::size_t validated = validate_runtime_thread_count(requested);
    return ordering == log_ordering::relaxed ? validated : 0U;
}

inline std::size_t async_logger::default_queue_shard_count() noexcept {
    const std::size_t hardware_threads = detail::hardware_thread_count();
    return hardware_threads == 0U
               ? 1U
               : detail::next_power_of_two(static_cast<std::size_t>(hardware_threads));
}

inline std::size_t async_logger::normalize_queue_shard_count(std::size_t requested) {
    const std::size_t shard_count = requested == 0U ? default_queue_shard_count() : requested;
    return detail::checked_next_power_of_two(shard_count == 0U ? 1U : shard_count,
                                             "async log queue shard count is too large");
}

inline std::size_t async_logger::validate_runtime_thread_count(std::size_t requested) {
    if (requested > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
        throw std::length_error("async log runtime thread count is out of range");
    }
    return requested;
}

inline std::size_t async_logger::queue_capacity_per_shard(std::size_t total_capacity,
                                                          std::size_t shard_count) noexcept {
    const std::size_t capacity = total_capacity == 0U ? 1U : total_capacity;
    const std::size_t per_shard = capacity / shard_count + (capacity % shard_count != 0U);
    return std::max<std::size_t>(2U, per_shard);
}

inline std::size_t
async_logger::queue_capacity_per_runtime_thread(std::size_t total_capacity,
                                                std::size_t thread_count) noexcept {
    if (thread_count == 0U) {
        return 0U;
    }
    const std::size_t capacity = total_capacity == 0U ? 1U : total_capacity;
    const std::size_t per_thread = capacity / thread_count + (capacity % thread_count != 0U);
    return std::max<std::size_t>(2U, per_thread);
}

inline std::size_t async_logger::record_capacity_per_shard(std::size_t queue_capacity,
                                                           std::size_t max_batch_size) {
    const std::size_t batch_capacity = max_batch_size == 0U ? 1U : max_batch_size;
    if (queue_capacity > std::numeric_limits<std::size_t>::max() - batch_capacity) {
        throw std::length_error("async log record pool capacity overflow");
    }
    return queue_capacity + batch_capacity;
}

inline std::size_t async_logger::ordered_record_capacity_per_producer_shard(
    std::size_t total_capacity, std::size_t shard_count, std::size_t max_batch_size) {
    if (shard_count == 0U) {
        return 0U;
    }
    return record_capacity_per_shard(queue_capacity_per_shard(total_capacity, shard_count),
                                     max_batch_size);
}

inline std::size_t async_logger::record_pool_slab_capacity(std::size_t requested,
                                                           std::size_t fallback) {
    if (requested == 0U) {
        return fallback == 0U ? 1U : fallback;
    }
    if (requested >= static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::length_error("async log record pool slab size is out of range");
    }
    return requested;
}

inline std::unique_ptr<detail::async_log_ordered_queue>
async_logger::make_ordered_queue(log_ordering ordering, std::size_t queue_capacity) {
    if (ordering == log_ordering::relaxed) {
        return nullptr;
    }
    return std::make_unique<detail::async_log_ordered_queue>(queue_capacity);
}

inline detail::async_log_producer_shard_storage
async_logger::make_ordered_producer_shards(std::size_t shard_count, std::size_t record_capacity,
                                           std::size_t record_pool_slab_object_count,
                                           std::size_t record_pool_local_cache_size) {
    detail::async_log_producer_shard_storage shards;
    if (shard_count == 0U) {
        return shards;
    }

    shards.reserve_exact(shard_count);
    record_capacity = record_pool_slab_capacity(record_pool_slab_object_count, record_capacity);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards.emplace_back(record_capacity, record_pool_local_cache_size);
    }
    return shards;
}

inline detail::async_log_queue_shard_storage async_logger::make_queue_shards(
    std::size_t shard_count, std::size_t capacity_per_shard, std::size_t max_batch_size,
    std::size_t record_pool_slab_object_count, std::size_t record_pool_local_cache_size) {
    detail::async_log_queue_shard_storage shards;
    if (shard_count == 0U) {
        return shards;
    }

    shards.reserve_exact(shard_count);
    const std::size_t record_capacity =
        record_pool_slab_capacity(record_pool_slab_object_count,
                                  record_capacity_per_shard(capacity_per_shard, max_batch_size));
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards.emplace_back(capacity_per_shard, record_capacity, record_pool_local_cache_size);
    }
    return shards;
}

inline detail::async_log_runtime_lane_storage async_logger::make_runtime_lanes(
    std::size_t thread_count, std::size_t capacity_per_thread, std::size_t max_batch_size,
    std::size_t record_pool_slab_object_count, std::size_t record_pool_local_cache_size) {
    detail::async_log_runtime_lane_storage lanes;
    if (thread_count == 0U) {
        return lanes;
    }

    lanes.reserve_exact(thread_count);
    const std::size_t record_capacity =
        record_pool_slab_capacity(record_pool_slab_object_count,
                                  record_capacity_per_shard(capacity_per_thread, max_batch_size));
    for (std::size_t i = 0; i < thread_count; ++i) {
        lanes.emplace_back(capacity_per_thread, record_capacity, record_pool_local_cache_size);
    }
    return lanes;
}

} // namespace af
