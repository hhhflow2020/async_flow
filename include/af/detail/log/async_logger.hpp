#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/async_log_config.hpp"
#include "af/detail/log/async_log_consumer.hpp"
#include "af/detail/log/async_log_drain_waiter.hpp"
#include "af/detail/log/async_log_lanes.hpp"
#include "af/detail/log/async_log_record_pool.hpp"
#include "af/detail/queue/queue_backoff.hpp"
#include "af/detail/thread/hardware_threads.hpp"

namespace af {

class RuntimeInstanceAbslAsyncLogSink;

class AsyncLogger {
public:
    explicit AsyncLogger(AsyncLogConfig config);

    AsyncLogger(const AsyncLogger &) = delete;
    AsyncLogger &operator=(const AsyncLogger &) = delete;

    ~AsyncLogger();

    [[nodiscard]] bool try_log(std::string_view message) noexcept;
    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept;
    [[nodiscard]] AsyncLogStats stats() const noexcept;
    [[nodiscard]] std::chrono::milliseconds fatal_flush_timeout() const noexcept;

private:
    friend class detail::RuntimeInstanceAsyncLogConsumerController;
    friend class RuntimeInstanceAbslAsyncLogSink;

    [[nodiscard]] bool try_log_from_runtime_thread(std::uint16_t thread_index,
                                                   std::string_view message) noexcept;

    template <typename Lane>
    [[nodiscard]] bool try_log_on_lane(Lane &lane, std::string_view message) noexcept;

    [[nodiscard]] bool start_bound_consumer(detail::AsyncLogConsumerWakeTarget &target) noexcept;
    void stop_bound_consumer_admission() noexcept;
    void finish_bound_consumer_shutdown() noexcept;
    [[nodiscard]] bool consumer_stop_requested() const noexcept;
    [[nodiscard]] std::size_t pending_record_count() const noexcept;
    [[nodiscard]] std::size_t ready_record_count() const noexcept;
    [[nodiscard]] std::size_t max_batch_size() const noexcept;

    struct ProducerShardCache {
        const AsyncLogger *logger{nullptr};
        std::uint64_t token{0};
        detail::AsyncLogQueueShard *shard{nullptr};
    };

    struct OrderedProducerShardCache {
        const AsyncLogger *logger{nullptr};
        std::uint64_t token{0};
        detail::AsyncLogProducerShard *shard{nullptr};
    };

    [[nodiscard]] static LogOrdering validate_ordering(LogOrdering ordering);
    [[nodiscard]] static std::size_t relaxed_queue_shard_count_for_ordering(LogOrdering ordering,
                                                                            std::size_t requested);
    [[nodiscard]] static std::size_t
    ordered_producer_shard_count_for_ordering(LogOrdering ordering, std::size_t requested);
    [[nodiscard]] static std::size_t runtime_thread_count_for_ordering(LogOrdering ordering,
                                                                       std::size_t requested);
    [[nodiscard]] static std::size_t default_queue_shard_count() noexcept;
    [[nodiscard]] static std::size_t normalize_queue_shard_count(std::size_t requested);
    [[nodiscard]] static std::size_t validate_runtime_thread_count(std::size_t requested);
    [[nodiscard]] static std::size_t queue_capacity_per_shard(std::size_t total_capacity,
                                                              std::size_t shard_count) noexcept;
    [[nodiscard]] static std::size_t
    queue_capacity_per_runtime_thread(std::size_t total_capacity,
                                      std::size_t thread_count) noexcept;
    [[nodiscard]] static std::size_t record_capacity_per_shard(std::size_t queue_capacity,
                                                               std::size_t max_batch_size);
    [[nodiscard]] static std::size_t
    ordered_record_capacity_per_producer_shard(std::size_t total_capacity, std::size_t shard_count,
                                               std::size_t max_batch_size);
    [[nodiscard]] static std::size_t record_pool_slab_capacity(std::size_t requested,
                                                               std::size_t fallback);
    [[nodiscard]] static std::unique_ptr<detail::AsyncLogOrderedQueue>
    make_ordered_queue(LogOrdering ordering, std::size_t queue_capacity);
    [[nodiscard]] static detail::AsyncLogProducerShardStorage
    make_ordered_producer_shards(std::size_t shard_count, std::size_t record_capacity,
                                 std::size_t record_pool_slab_object_count,
                                 std::size_t record_pool_local_cache_size);
    [[nodiscard]] static detail::AsyncLogQueueShardStorage
    make_queue_shards(std::size_t shard_count, std::size_t capacity_per_shard,
                      std::size_t max_batch_size, std::size_t record_pool_slab_object_count,
                      std::size_t record_pool_local_cache_size);
    [[nodiscard]] static detail::AsyncLogRuntimeLaneStorage
    make_runtime_lanes(std::size_t thread_count, std::size_t capacity_per_thread,
                       std::size_t max_batch_size, std::size_t record_pool_slab_object_count,
                       std::size_t record_pool_local_cache_size);

    [[nodiscard]] detail::AsyncLogProducerShard &ordered_producer_shard() noexcept;
    [[nodiscard]] detail::AsyncLogQueueShard &producer_shard() noexcept;
    [[nodiscard]] bool try_log_ordered(std::string_view message) noexcept;

    template <typename Lane> static void record_accepted(Lane &lane) noexcept;
    template <typename Lane> static void record_dropped(Lane &lane) noexcept;

    template <typename Lane>
    [[nodiscard]] detail::LogRecord *acquire_record(Lane &lane, std::string_view message) noexcept;

    template <typename Lane>
    [[nodiscard]] bool push_record(Lane &lane, detail::LogRecord *record) noexcept;

    [[nodiscard]] bool push_ordered_record(detail::LogRecord *record) noexcept;
    static void release_record(detail::LogRecord *record) noexcept;
    static void release_unpublished_record(detail::AsyncLogQueueShard &,
                                           detail::LogRecord *record) noexcept;
    static void release_unpublished_record(detail::AsyncLogProducerShard &,
                                           detail::LogRecord *record) noexcept;
    static void release_unpublished_record(detail::AsyncLogRuntimeLane &lane,
                                           detail::LogRecord *record) noexcept;

    void shutdown() noexcept;
    void abandon_pending_record() noexcept;
    [[nodiscard]] bool drain_some(std::vector<detail::LogRecord *> &batch,
                                  std::size_t max_write_batches) noexcept;
    void collect_batch(std::vector<detail::LogRecord *> &batch, std::size_t max_records) noexcept;
    void collect_ordered_batch(std::vector<detail::LogRecord *> &batch,
                               std::size_t max_records) noexcept;
    void collect_shard_batch(std::vector<detail::LogRecord *> &batch,
                             std::size_t max_records) noexcept;
    void collect_runtime_batch(std::vector<detail::LogRecord *> &batch,
                               std::size_t max_records) noexcept;
    void flush_backends() noexcept;
    [[nodiscard]] bool
    flush_backends_until(std::chrono::steady_clock::time_point deadline) noexcept;
    void shutdown_backends() noexcept;
    void notify_consumer() noexcept;

    static inline std::atomic<std::uint64_t> next_cache_token_{1};

    const std::uint64_t cache_token_;
    const LogOrdering ordering_;
    const std::size_t queue_shard_count_;
    const std::size_t queue_shard_mask_;
    const std::size_t ordered_producer_shard_count_;
    const std::size_t ordered_producer_shard_mask_;
    const std::size_t runtime_thread_count_;
    std::unique_ptr<detail::AsyncLogOrderedQueue> ordered_queue_;
    detail::AsyncLogProducerShardStorage ordered_producer_shards_;
    detail::AsyncLogQueueShardStorage queue_shards_;
    detail::AsyncLogRuntimeLaneStorage runtime_lanes_;
    const std::size_t max_batch_size_;
    const std::size_t overflow_spin_count_;
    const LogOverflowPolicy overflow_policy_;
    const std::chrono::milliseconds flush_poll_interval_;
    const std::chrono::milliseconds fatal_flush_timeout_;
    std::vector<std::unique_ptr<LogBackend>> backends_;

    alignas(detail::hardware_cache_line_size) std::atomic<bool> started_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> accepting_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> stopping_{false};
    alignas(detail::hardware_cache_line_size)
        std::atomic<detail::AsyncLogConsumerWakeTarget *> consumer_wake_target_{nullptr};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> next_ordered_producer_shard_{
        0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> next_producer_shard_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> pending_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> ready_{0};

    std::size_t next_drain_shard_{0};
    std::size_t next_runtime_drain_thread_{0};
    bool prefer_runtime_drain_{true};
    detail::AsyncLogDrainWaiter drain_waiter_;
};

using async_logger = AsyncLogger;

} // namespace af

#include "af/detail/log/async_logger_config_impl.hpp"
#include "af/detail/log/async_logger_consumer_impl.hpp"
#include "af/detail/log/async_logger_producer_impl.hpp"
