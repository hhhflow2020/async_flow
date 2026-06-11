#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/detail/log/log_backend.hpp"

namespace af {

enum class log_overflow_policy : std::uint8_t {
    drop_newest,
    block,
};

enum class log_ordering : std::uint8_t {
    ordered,
    relaxed,
};

enum class log_level : std::int8_t {
    info,
    warning,
    error,
    fatal,
    off,
};

inline constexpr std::size_t async_log_record_pool_max_local_cache_size = 4096;

struct async_log_config {
    static constexpr std::size_t default_queue_capacity = 1U << 16U;
    static constexpr std::size_t auto_queue_shard_count = 0U;
    static constexpr std::size_t auto_runtime_thread_count = 0U;

    [[nodiscard]] static async_log_config ordered() noexcept {
        async_log_config config;
        config.use_ordered();
        return config;
    }

    [[nodiscard]] static async_log_config ordered(std::size_t producer_shard_count) noexcept {
        async_log_config config;
        config.use_ordered(producer_shard_count);
        return config;
    }

    [[nodiscard]] static async_log_config relaxed() noexcept {
        async_log_config config;
        config.use_relaxed();
        return config;
    }

    [[nodiscard]] static async_log_config
    relaxed(std::size_t runtime_threads,
            std::size_t external_shard_count = auto_queue_shard_count) noexcept {
        async_log_config config;
        config.use_relaxed(runtime_threads, external_shard_count);
        return config;
    }

    // Ordered keeps one backend-visible enqueue order. Relaxed trades that
    // global order for runtime lanes and sharded external MPSC queues.
    async_log_config &
    use_ordered(std::size_t producer_shard_count = auto_queue_shard_count) noexcept {
        ordering = log_ordering::ordered;
        queue_shard_count = producer_shard_count;
        runtime_thread_count = 0U;
        return *this;
    }

    async_log_config &
    use_relaxed(std::size_t runtime_threads = auto_runtime_thread_count,
                std::size_t external_shard_count = auto_queue_shard_count) noexcept {
        ordering = log_ordering::relaxed;
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
    log_overflow_policy overflow_policy{log_overflow_policy::drop_newest};
    log_ordering ordering{log_ordering::ordered};
    log_level min_level{log_level::info};
    std::chrono::milliseconds flush_poll_interval{std::chrono::milliseconds(1)};
    std::chrono::milliseconds fatal_flush_timeout{std::chrono::milliseconds(200)};
    std::vector<std::unique_ptr<log_backend>> backends;
};

struct async_log_stats {
    std::uint64_t accepted{0};
    std::uint64_t dropped{0};
};

} // namespace af
