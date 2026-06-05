#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/detail/log/log_backend.hpp"

namespace af {

enum class LogOverflowPolicy : std::uint8_t {
    DropNewest,
    Block,
    drop_newest = DropNewest,
    block = Block,
};

enum class LogOrdering : std::uint8_t {
    Ordered,
    Relaxed,
    ordered = Ordered,
    relaxed = Relaxed,
};

using log_overflow_policy = LogOverflowPolicy;
using log_ordering = LogOrdering;

struct AsyncLogConfig {
    static constexpr std::size_t default_queue_capacity = 1U << 16U;
    static constexpr std::size_t auto_queue_shard_count = 0U;
    static constexpr std::size_t auto_runtime_thread_count = 0U;

    [[nodiscard]] static AsyncLogConfig ordered() noexcept {
        AsyncLogConfig config;
        config.use_ordered();
        return config;
    }

    [[nodiscard]] static AsyncLogConfig ordered(std::size_t producer_shard_count) noexcept {
        AsyncLogConfig config;
        config.use_ordered(producer_shard_count);
        return config;
    }

    [[nodiscard]] static AsyncLogConfig relaxed() noexcept {
        AsyncLogConfig config;
        config.use_relaxed();
        return config;
    }

    [[nodiscard]] static AsyncLogConfig
    relaxed(std::size_t runtime_threads,
            std::size_t external_shard_count = auto_queue_shard_count) noexcept {
        AsyncLogConfig config;
        config.use_relaxed(runtime_threads, external_shard_count);
        return config;
    }

    // Ordered keeps one backend-visible enqueue order. Relaxed trades that
    // global order for runtime lanes and sharded external MPSC queues.
    AsyncLogConfig &
    use_ordered(std::size_t producer_shard_count = auto_queue_shard_count) noexcept {
        ordering = LogOrdering::Ordered;
        queue_shard_count = producer_shard_count;
        runtime_thread_count = 0U;
        return *this;
    }

    AsyncLogConfig &
    use_relaxed(std::size_t runtime_threads = auto_runtime_thread_count,
                std::size_t external_shard_count = auto_queue_shard_count) noexcept {
        ordering = LogOrdering::Relaxed;
        queue_shard_count = external_shard_count;
        runtime_thread_count = runtime_threads;
        return *this;
    }

    std::size_t queue_capacity{default_queue_capacity};
    std::size_t queue_shard_count{0};
    std::size_t runtime_thread_count{0};
    std::size_t runtime_lane_capacity{0};
    std::size_t record_pool_local_cache_size{256};
    std::size_t record_pool_slab_object_count{0};
    std::size_t max_batch_size{256};
    std::size_t max_consumer_batches_per_run{64};
    std::size_t overflow_spin_count{64};
    LogOverflowPolicy overflow_policy{LogOverflowPolicy::DropNewest};
    LogOrdering ordering{LogOrdering::Ordered};
    std::chrono::milliseconds flush_poll_interval{std::chrono::milliseconds(1)};
    std::chrono::milliseconds fatal_flush_timeout{std::chrono::milliseconds(200)};
    std::vector<std::unique_ptr<LogBackend>> backends;
};

struct AsyncLogStats {
    std::uint64_t accepted{0};
    std::uint64_t dropped{0};
};

using log_overflow_policy = LogOverflowPolicy;
using log_ordering = LogOrdering;
using async_log_config = AsyncLogConfig;
using async_log_stats = AsyncLogStats;

} // namespace af
