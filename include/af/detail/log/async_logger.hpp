#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/async_log_drain_waiter.hpp"
#include "af/detail/log/async_log_lanes.hpp"
#include "af/detail/log/async_log_record_pool.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/queue/queue_backoff.hpp"
#include "af/detail/thread/hardware_threads.hpp"

namespace af {

template <typename RuntimeT> class RuntimeAbslAsyncLogSink;
class RuntimeInstanceAbslAsyncLogSink;

namespace detail {

class AsyncLogConsumerWakeTarget {
public:
    AsyncLogConsumerWakeTarget() = default;
    AsyncLogConsumerWakeTarget(const AsyncLogConsumerWakeTarget &) = delete;
    AsyncLogConsumerWakeTarget &operator=(const AsyncLogConsumerWakeTarget &) = delete;
    virtual ~AsyncLogConsumerWakeTarget() = default;

    [[nodiscard]] virtual bool wake_async_log_consumer() noexcept = 0;
};

class AsyncLogConsumerController {
public:
    AsyncLogConsumerController() = default;
    AsyncLogConsumerController(const AsyncLogConsumerController &) = delete;
    AsyncLogConsumerController &operator=(const AsyncLogConsumerController &) = delete;
    virtual ~AsyncLogConsumerController() = default;

    virtual void shutdown() noexcept = 0;
};

template <typename RuntimeT> class RuntimeAsyncLogConsumerController;
class RuntimeInstanceAsyncLogConsumerController;

} // namespace detail

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

class AsyncLogger {
public:
    explicit AsyncLogger(AsyncLogConfig config)
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
              config.record_pool_slab_object_count)),
          queue_shards_(make_queue_shards(
              queue_shard_count_,
              queue_shard_count_ == 0U
                  ? 0U
                  : queue_capacity_per_shard(config.queue_capacity, queue_shard_count_),
              config.max_batch_size, config.record_pool_slab_object_count)),
          runtime_lanes_(make_runtime_lanes(
              runtime_thread_count_,
              queue_capacity_per_runtime_thread(config.runtime_lane_capacity == 0U
                                                    ? config.queue_capacity
                                                    : config.runtime_lane_capacity,
                                                runtime_thread_count_),
              config.max_batch_size, config.record_pool_slab_object_count)),
          max_batch_size_(config.max_batch_size == 0U ? 1U : config.max_batch_size),
          overflow_spin_count_(config.overflow_spin_count),
          overflow_policy_(config.overflow_policy),
          flush_poll_interval_(config.flush_poll_interval),
          fatal_flush_timeout_(config.fatal_flush_timeout), backends_(std::move(config.backends)) {}

    AsyncLogger(const AsyncLogger &) = delete;
    AsyncLogger &operator=(const AsyncLogger &) = delete;

    ~AsyncLogger() {
        shutdown();
    }

    [[nodiscard]] bool try_log(std::string_view message) noexcept {
        if (ordering_ == LogOrdering::Ordered) [[likely]] {
            return try_log_ordered(message);
        }

        detail::AsyncLogQueueShard &shard = producer_shard();
        return try_log_on_lane(shard, message);
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        if (!drain_waiter_.wait_until_drained(pending_, deadline, flush_poll_interval_,
                                              [this] { notify_consumer(); })) {
            return false;
        }
        return flush_backends_until(deadline);
    }

    [[nodiscard]] AsyncLogStats stats() const noexcept {
        AsyncLogStats result;
        for (const detail::AsyncLogProducerShard &shard : ordered_producer_shards_) {
            result.accepted += shard.accepted.load();
            result.dropped += shard.dropped.load();
        }
        for (const detail::AsyncLogQueueShard &shard : queue_shards_) {
            result.accepted += shard.accepted.load();
            result.dropped += shard.dropped.load();
        }
        for (const detail::AsyncLogRuntimeLane &lane : runtime_lanes_) {
            result.accepted += lane.accepted.load();
            result.dropped += lane.dropped.load();
        }
        return result;
    }

    [[nodiscard]] std::chrono::milliseconds fatal_flush_timeout() const noexcept {
        return fatal_flush_timeout_;
    }

private:
    template <typename RuntimeT> friend class detail::RuntimeAsyncLogConsumerController;
    friend class detail::RuntimeInstanceAsyncLogConsumerController;
    template <typename RuntimeT> friend class RuntimeAbslAsyncLogSink;
    friend class RuntimeInstanceAbslAsyncLogSink;

    [[nodiscard]] bool try_log_from_runtime_thread(std::uint16_t thread_index,
                                                   std::string_view message) noexcept {
        if (ordering_ == LogOrdering::Ordered) [[likely]] {
            return try_log(message);
        }

        if (thread_index >= runtime_thread_count_) [[unlikely]] {
            return try_log(message);
        }

        detail::AsyncLogRuntimeLane &lane = *runtime_lanes_[thread_index];
        return try_log_on_lane(lane, message);
    }

    template <typename Lane>
    [[nodiscard]] bool try_log_on_lane(Lane &lane, std::string_view message) noexcept {
        if (!accepting_.load(std::memory_order_acquire)) {
            record_dropped(lane);
            return false;
        }

        detail::LogRecord *record = acquire_record(lane, message);
        if (record == nullptr) {
            record_dropped(lane);
            return false;
        }

        pending_.fetch_add(1U, std::memory_order_relaxed);
        if (!push_record(lane, record)) {
            release_unpublished_record(lane, record);
            abandon_pending_record();
            record_dropped(lane);
            return false;
        }

        record_accepted(lane);
        const auto previous_ready = ready_.fetch_add(1U, std::memory_order_release);
        if (previous_ready == 0U) {
            notify_consumer();
        }
        return true;
    }

    [[nodiscard]] bool start_bound_consumer(detail::AsyncLogConsumerWakeTarget &target) noexcept {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            return false;
        }

        consumer_wake_target_.store(&target, std::memory_order_release);
        stopping_.store(false, std::memory_order_relaxed);
        accepting_.store(true, std::memory_order_release);
        return true;
    }

    void stop_bound_consumer_admission() noexcept {
        accepting_.store(false, std::memory_order_release);
        stopping_.store(true, std::memory_order_relaxed);
    }

    void finish_bound_consumer_shutdown() noexcept {
        consumer_wake_target_.store(nullptr, std::memory_order_release);
        shutdown_backends();
        started_.store(false, std::memory_order_release);
    }

    [[nodiscard]] bool consumer_stop_requested() const noexcept {
        return stopping_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t pending_record_count() const noexcept {
        return pending_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t ready_record_count() const noexcept {
        return ready_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t max_batch_size() const noexcept {
        return max_batch_size_;
    }

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

    [[nodiscard]] static LogOrdering validate_ordering(LogOrdering ordering) {
        switch (ordering) {
        case LogOrdering::Ordered:
        case LogOrdering::Relaxed:
            return ordering;
        }
        throw std::invalid_argument("invalid async log ordering");
    }

    [[nodiscard]] static std::size_t relaxed_queue_shard_count_for_ordering(LogOrdering ordering,
                                                                            std::size_t requested) {
        if (ordering == LogOrdering::Ordered) {
            return 0U;
        }
        return normalize_queue_shard_count(requested);
    }

    [[nodiscard]] static std::size_t
    ordered_producer_shard_count_for_ordering(LogOrdering ordering, std::size_t requested) {
        if (ordering == LogOrdering::Relaxed) {
            return 0U;
        }
        return normalize_queue_shard_count(requested);
    }

    [[nodiscard]] static std::size_t runtime_thread_count_for_ordering(LogOrdering ordering,
                                                                       std::size_t requested) {
        const std::size_t validated = validate_runtime_thread_count(requested);
        return ordering == LogOrdering::Relaxed ? validated : 0U;
    }

    [[nodiscard]] static std::size_t default_queue_shard_count() noexcept {
        const std::size_t hardware_threads = detail::hardware_thread_count();
        return hardware_threads == 0U
                   ? 1U
                   : detail::next_power_of_two(static_cast<std::size_t>(hardware_threads));
    }

    [[nodiscard]] static std::size_t normalize_queue_shard_count(std::size_t requested) {
        const std::size_t shard_count = requested == 0U ? default_queue_shard_count() : requested;
        return detail::checked_next_power_of_two(shard_count == 0U ? 1U : shard_count,
                                                 "async log queue shard count is too large");
    }

    [[nodiscard]] static std::size_t validate_runtime_thread_count(std::size_t requested) {
        if (requested > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
            throw std::length_error("async log runtime thread count is out of range");
        }
        return requested;
    }

    [[nodiscard]] static std::size_t queue_capacity_per_shard(std::size_t total_capacity,
                                                              std::size_t shard_count) noexcept {
        const std::size_t capacity = total_capacity == 0U ? 1U : total_capacity;
        const std::size_t per_shard = capacity / shard_count + (capacity % shard_count != 0U);
        return std::max<std::size_t>(2U, per_shard);
    }

    [[nodiscard]] static std::size_t
    queue_capacity_per_runtime_thread(std::size_t total_capacity,
                                      std::size_t thread_count) noexcept {
        if (thread_count == 0U) {
            return 0U;
        }
        const std::size_t capacity = total_capacity == 0U ? 1U : total_capacity;
        const std::size_t per_thread = capacity / thread_count + (capacity % thread_count != 0U);
        return std::max<std::size_t>(2U, per_thread);
    }

    [[nodiscard]] static std::size_t record_capacity_per_shard(std::size_t queue_capacity,
                                                               std::size_t max_batch_size) {
        const std::size_t batch_capacity = max_batch_size == 0U ? 1U : max_batch_size;
        if (queue_capacity > std::numeric_limits<std::size_t>::max() - batch_capacity) {
            throw std::length_error("async log record pool capacity overflow");
        }
        return queue_capacity + batch_capacity;
    }

    [[nodiscard]] static std::size_t
    ordered_record_capacity_per_producer_shard(std::size_t total_capacity, std::size_t shard_count,
                                               std::size_t max_batch_size) {
        if (shard_count == 0U) {
            return 0U;
        }
        return record_capacity_per_shard(queue_capacity_per_shard(total_capacity, shard_count),
                                         max_batch_size);
    }

    [[nodiscard]] static std::size_t record_pool_slab_capacity(std::size_t requested,
                                                               std::size_t fallback) {
        if (requested == 0U) {
            return fallback == 0U ? 1U : fallback;
        }
        if (requested >= static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
            throw std::length_error("async log record pool slab size is out of range");
        }
        return requested;
    }

    [[nodiscard]] static std::unique_ptr<detail::AsyncLogOrderedQueue>
    make_ordered_queue(LogOrdering ordering, std::size_t queue_capacity) {
        if (ordering == LogOrdering::Relaxed) {
            return nullptr;
        }
        return std::make_unique<detail::AsyncLogOrderedQueue>(queue_capacity);
    }

    [[nodiscard]] static detail::AsyncLogProducerShardStorage
    make_ordered_producer_shards(std::size_t shard_count, std::size_t record_capacity,
                                 std::size_t record_pool_slab_object_count) {
        detail::AsyncLogProducerShardStorage shards;
        if (shard_count == 0U) {
            return shards;
        }

        shards.reserve_exact(shard_count);
        record_capacity = record_pool_slab_capacity(record_pool_slab_object_count, record_capacity);
        for (std::size_t i = 0; i < shard_count; ++i) {
            shards.emplace_back(record_capacity);
        }
        return shards;
    }

    [[nodiscard]] static detail::AsyncLogQueueShardStorage
    make_queue_shards(std::size_t shard_count, std::size_t capacity_per_shard,
                      std::size_t max_batch_size, std::size_t record_pool_slab_object_count) {
        detail::AsyncLogQueueShardStorage shards;
        if (shard_count == 0U) {
            return shards;
        }

        shards.reserve_exact(shard_count);
        const std::size_t record_capacity = record_pool_slab_capacity(
            record_pool_slab_object_count,
            record_capacity_per_shard(capacity_per_shard, max_batch_size));
        for (std::size_t i = 0; i < shard_count; ++i) {
            shards.emplace_back(capacity_per_shard, record_capacity);
        }
        return shards;
    }

    [[nodiscard]] static detail::AsyncLogRuntimeLaneStorage
    make_runtime_lanes(std::size_t thread_count, std::size_t capacity_per_thread,
                       std::size_t max_batch_size, std::size_t record_pool_slab_object_count) {
        detail::AsyncLogRuntimeLaneStorage lanes;
        if (thread_count == 0U) {
            return lanes;
        }

        lanes.reserve_exact(thread_count);
        const std::size_t record_capacity = record_pool_slab_capacity(
            record_pool_slab_object_count,
            record_capacity_per_shard(capacity_per_thread, max_batch_size));
        for (std::size_t i = 0; i < thread_count; ++i) {
            lanes.emplace_back(capacity_per_thread, record_capacity);
        }
        return lanes;
    }

    [[nodiscard]] detail::AsyncLogProducerShard &ordered_producer_shard() noexcept {
        thread_local OrderedProducerShardCache cache;
        if (cache.logger != this || cache.token != cache_token_) [[unlikely]] {
            const std::size_t shard_index =
                next_ordered_producer_shard_.fetch_add(1U, std::memory_order_relaxed) &
                ordered_producer_shard_mask_;
            cache.logger = this;
            cache.token = cache_token_;
            cache.shard = ordered_producer_shards_[shard_index];
        }
        AF_ASSERT(cache.shard != nullptr);
        return *cache.shard;
    }

    [[nodiscard]] detail::AsyncLogQueueShard &producer_shard() noexcept {
        thread_local ProducerShardCache cache;
        if (cache.logger != this || cache.token != cache_token_) [[unlikely]] {
            const std::size_t shard_index =
                next_producer_shard_.fetch_add(1U, std::memory_order_relaxed) & queue_shard_mask_;
            cache.logger = this;
            cache.token = cache_token_;
            cache.shard = queue_shards_[shard_index];
        }
        AF_ASSERT(cache.shard != nullptr);
        return *cache.shard;
    }

    [[nodiscard]] bool try_log_ordered(std::string_view message) noexcept {
        detail::AsyncLogProducerShard &producer = ordered_producer_shard();
        if (!accepting_.load(std::memory_order_acquire)) {
            record_dropped(producer);
            return false;
        }

        detail::LogRecord *record = acquire_record(producer, message);
        if (record == nullptr) {
            record_dropped(producer);
            return false;
        }

        pending_.fetch_add(1U, std::memory_order_relaxed);
        if (!push_ordered_record(record)) {
            release_unpublished_record(producer, record);
            abandon_pending_record();
            record_dropped(producer);
            return false;
        }

        record_accepted(producer);
        const auto previous_ready = ready_.fetch_add(1U, std::memory_order_release);
        if (previous_ready == 0U) {
            notify_consumer();
        }
        return true;
    }

    template <typename Lane> static void record_accepted(Lane &lane) noexcept {
        lane.accepted.add(1U);
    }

    template <typename Lane> static void record_dropped(Lane &lane) noexcept {
        lane.dropped.add(1U);
    }

    template <typename Lane>
    [[nodiscard]] detail::LogRecord *acquire_record(Lane &lane, std::string_view message) noexcept {
        if (overflow_policy_ == LogOverflowPolicy::DropNewest) {
            return lane.records.try_acquire(message);
        }

        detail::QueueFullBackoff backoff(overflow_spin_count_);
        while (accepting_.load(std::memory_order_acquire)) {
            if (detail::LogRecord *record = lane.records.try_acquire(message); record != nullptr) {
                return record;
            }
            backoff.wait();
        }
        return nullptr;
    }

    template <typename Lane>
    [[nodiscard]] bool push_record(Lane &lane, detail::LogRecord *record) noexcept {
        if (overflow_policy_ == LogOverflowPolicy::DropNewest) {
            return accepting_.load(std::memory_order_acquire) && lane.queue.try_push(record);
        }

        detail::QueueFullBackoff backoff(overflow_spin_count_);
        while (accepting_.load(std::memory_order_acquire)) {
            if (lane.queue.try_push(record)) {
                return true;
            }
            backoff.wait();
        }
        return false;
    }

    [[nodiscard]] bool push_ordered_record(detail::LogRecord *record) noexcept {
        AF_ASSERT(ordered_queue_ != nullptr);
        if (overflow_policy_ == LogOverflowPolicy::DropNewest) {
            return accepting_.load(std::memory_order_acquire) &&
                   ordered_queue_->queue.try_push(record);
        }

        detail::QueueFullBackoff backoff(overflow_spin_count_);
        while (accepting_.load(std::memory_order_acquire)) {
            if (ordered_queue_->queue.try_push(record)) {
                return true;
            }
            backoff.wait();
        }
        return false;
    }

    static void release_record(detail::LogRecord *record) noexcept {
        detail::release_async_log_record(record);
    }

    static void release_unpublished_record(detail::AsyncLogQueueShard &,
                                           detail::LogRecord *record) noexcept {
        release_record(record);
    }

    static void release_unpublished_record(detail::AsyncLogProducerShard &,
                                           detail::LogRecord *record) noexcept {
        release_record(record);
    }

    static void release_unpublished_record(detail::AsyncLogRuntimeLane &lane,
                                           detail::LogRecord *record) noexcept {
        static_cast<void>(lane);
        release_record(record);
    }

    void shutdown() noexcept {
        const bool was_started = started_.exchange(false, std::memory_order_acq_rel);
        accepting_.store(false, std::memory_order_release);
        stopping_.store(true, std::memory_order_relaxed);
        notify_consumer();
        if (was_started) {
            shutdown_backends();
        }
    }

    void abandon_pending_record() noexcept {
        const auto previous = pending_.fetch_sub(1U, std::memory_order_relaxed);
        AF_ASSERT(previous != 0U);
        if (previous == 1U) {
            drain_waiter_.notify_drained();
            notify_consumer();
        }
    }

    [[nodiscard]] bool drain_some(std::vector<detail::LogRecord *> &batch,
                                  std::size_t max_write_batches) noexcept {
        if (max_write_batches == 0U) {
            max_write_batches = 1U;
        }

        bool drained_any = false;
        for (;;) {
            if (max_write_batches == 0U) {
                return drained_any;
            }
            const std::size_t ready_records = ready_.load(std::memory_order_acquire);
            if (ready_records == 0U) {
                return drained_any;
            }
            batch.clear();
            collect_batch(batch, std::min(max_batch_size_, ready_records));

            if (batch.empty()) {
                return drained_any;
            }

            for (auto &backend : backends_) {
                backend->write_batch(
                    af::Span<detail::LogRecord *const>(batch.data(), batch.size()));
            }

            const auto drained = batch.size();
            detail::release_async_log_records(
                af::Span<detail::LogRecord *const>(batch.data(), drained));
            const auto previous_ready = ready_.fetch_sub(drained, std::memory_order_relaxed);
            AF_ASSERT(previous_ready >= drained);
            const auto previous = pending_.fetch_sub(drained, std::memory_order_release);
            AF_ASSERT(previous >= drained);
            if (previous == drained) {
                drain_waiter_.notify_drained();
            }

            drained_any = true;
            --max_write_batches;
        }
    }

    void collect_batch(std::vector<detail::LogRecord *> &batch, std::size_t max_records) noexcept {
        if (ordering_ == LogOrdering::Ordered) [[likely]] {
            collect_ordered_batch(batch, max_records);
            return;
        }

        if (prefer_runtime_drain_) {
            collect_runtime_batch(batch, max_records);
            collect_shard_batch(batch, max_records);
        } else {
            collect_shard_batch(batch, max_records);
            collect_runtime_batch(batch, max_records);
        }
        prefer_runtime_drain_ = !prefer_runtime_drain_;
    }

    void collect_ordered_batch(std::vector<detail::LogRecord *> &batch,
                               std::size_t max_records) noexcept {
        AF_ASSERT(ordered_queue_ != nullptr);
        constexpr std::size_t max_queue_drain_count = 64;
        std::array<detail::LogRecord *, max_queue_drain_count> drained;
        while (batch.size() < max_records) {
            const std::size_t count = ordered_queue_->queue.try_pop_many(
                drained.data(), std::min(drained.size(), max_records - batch.size()));
            if (count == 0U) {
                return;
            }
            batch.insert(batch.end(), drained.data(), drained.data() + count);
        }
    }

    void collect_shard_batch(std::vector<detail::LogRecord *> &batch,
                             std::size_t max_records) noexcept {
        constexpr std::size_t max_queue_drain_count = 64;
        std::array<detail::LogRecord *, max_queue_drain_count> drained;
        std::size_t empty_visits = 0;
        while (batch.size() < max_records && empty_visits < queue_shard_count_) {
            detail::AsyncLogQueueShard &shard = *queue_shards_[next_drain_shard_];
            next_drain_shard_ = (next_drain_shard_ + 1U) & queue_shard_mask_;

            const std::size_t count = shard.queue.try_pop_many(
                drained.data(), std::min(drained.size(), max_records - batch.size()));
            if (count == 0U) {
                ++empty_visits;
                continue;
            }

            empty_visits = 0;
            batch.insert(batch.end(), drained.data(), drained.data() + count);
        }
    }

    void collect_runtime_batch(std::vector<detail::LogRecord *> &batch,
                               std::size_t max_records) noexcept {
        if (runtime_thread_count_ == 0U) {
            return;
        }

        constexpr std::size_t max_queue_drain_count = 64;
        std::array<detail::LogRecord *, max_queue_drain_count> drained;
        std::size_t empty_visits = 0;
        while (batch.size() < max_records && empty_visits < runtime_thread_count_) {
            detail::AsyncLogRuntimeLane &lane = *runtime_lanes_[next_runtime_drain_thread_];
            ++next_runtime_drain_thread_;
            if (next_runtime_drain_thread_ == runtime_thread_count_) {
                next_runtime_drain_thread_ = 0U;
            }

            const std::size_t count = lane.queue.try_pop_many(
                drained.data(), std::min(drained.size(), max_records - batch.size()));
            if (count == 0U) {
                ++empty_visits;
                continue;
            }

            empty_visits = 0;
            batch.insert(batch.end(), drained.data(), drained.data() + count);
        }
    }

    void flush_backends() noexcept {
        for (auto &backend : backends_) {
            backend->flush();
        }
    }

    [[nodiscard]] bool
    flush_backends_until(std::chrono::steady_clock::time_point deadline) noexcept {
        for (auto &backend : backends_) {
            const auto now = std::chrono::steady_clock::now();
            const auto remaining =
                now >= deadline
                    ? std::chrono::milliseconds(0)
                    : std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            if (!backend->flush(remaining)) {
                return false;
            }
        }
        return true;
    }

    void shutdown_backends() noexcept {
        for (auto &backend : backends_) {
            backend->shutdown();
        }
    }

    void notify_consumer() noexcept {
        detail::AsyncLogConsumerWakeTarget *target =
            consumer_wake_target_.load(std::memory_order_acquire);
        if (target != nullptr) {
            static_cast<void>(target->wake_async_log_consumer());
        }
    }

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

} // namespace af
