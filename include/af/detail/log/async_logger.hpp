#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/memory/contiguous_object_storage.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"
#include "af/detail/queue/bounded_spsc_queue.hpp"
#include "af/detail/queue/queue_backoff.hpp"
#include "af/detail/thread/thread_name.hpp"

namespace af {

enum class LogOverflowPolicy : std::uint8_t {
    DropNewest,
    Block,
};

struct AsyncLogConfig {
    std::size_t queue_capacity{1U << 16U};
    std::size_t queue_shard_count{0};
    std::size_t runtime_thread_count{0};
    std::size_t runtime_queue_capacity{0};
    std::size_t max_batch_size{256};
    std::size_t overflow_spin_count{64};
    LogOverflowPolicy overflow_policy{LogOverflowPolicy::DropNewest};
    std::chrono::milliseconds flush_poll_interval{std::chrono::milliseconds(1)};
    std::chrono::milliseconds fatal_flush_timeout{std::chrono::milliseconds(200)};
    std::string consumer_thread_name{"log"};
    bool initialize_absl_log{true};
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
          queue_shard_count_(normalize_queue_shard_count(config.queue_shard_count)),
          queue_shard_mask_(queue_shard_count_ - 1U),
          runtime_thread_count_(validate_runtime_thread_count(config.runtime_thread_count)),
          queue_shards_(
              make_queue_shards(queue_shard_count_,
                                queue_capacity_per_shard(config.queue_capacity, queue_shard_count_),
                                config.max_batch_size)),
          runtime_lanes_(make_runtime_lanes(
              runtime_thread_count_,
              queue_capacity_per_runtime_thread(config.runtime_queue_capacity == 0U
                                                    ? config.queue_capacity
                                                    : config.runtime_queue_capacity,
                                                runtime_thread_count_),
              config.max_batch_size)),
          max_batch_size_(config.max_batch_size == 0U ? 1U : config.max_batch_size),
          overflow_spin_count_(config.overflow_spin_count),
          overflow_policy_(config.overflow_policy),
          flush_poll_interval_(config.flush_poll_interval),
          fatal_flush_timeout_(config.fatal_flush_timeout),
          consumer_thread_name_(std::move(config.consumer_thread_name)),
          backends_(std::move(config.backends)) {}

    AsyncLogger(const AsyncLogger &) = delete;
    AsyncLogger &operator=(const AsyncLogger &) = delete;

    ~AsyncLogger() {
        shutdown();
    }

    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            return;
        }
        accepting_.store(true, std::memory_order_release);
        worker_ = std::thread([this] {
            detail::set_current_thread_name(consumer_thread_name_, 0U);
            worker_main();
        });
    }

    [[nodiscard]] bool try_log(std::string_view message) noexcept {
        QueueShard &shard = producer_shard();
        return try_log_on_lane(shard, message);
    }

    [[nodiscard]] bool try_log_from_runtime_thread(std::uint16_t thread_index,
                                                   std::string_view message) noexcept {
        if (thread_index >= runtime_thread_count_) [[unlikely]] {
            return try_log(message);
        }

        RuntimeLane &lane = *runtime_lanes_[thread_index];
        return try_log_on_lane(lane, message);
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock lock(wait_mutex_);
        while (pending_.load(std::memory_order_acquire) != 0U) {
            if (drained_cv_.wait_until(lock, deadline) == std::cv_status::timeout &&
                pending_.load(std::memory_order_acquire) != 0U) {
                return false;
            }
        }
        lock.unlock();
        return flush_backends_until(deadline);
    }

    void shutdown() noexcept {
        const bool was_accepting = accepting_.exchange(false, std::memory_order_acq_rel);
        stopping_.store(true, std::memory_order_release);
        wake_cv_.notify_one();
        if (worker_.joinable()) {
            worker_.join();
        }
        if (was_accepting || started_.load(std::memory_order_acquire)) {
            shutdown_backends();
        }
    }

    [[nodiscard]] AsyncLogStats stats() const noexcept {
        AsyncLogStats result;
        for (const QueueShard &shard : queue_shards_) {
            result.accepted += shard.accepted.load();
            result.dropped += shard.dropped.load();
        }
        for (const RuntimeLane &lane : runtime_lanes_) {
            result.accepted += lane.accepted.load();
            result.dropped += lane.dropped.load();
        }
        return result;
    }

    [[nodiscard]] std::chrono::milliseconds fatal_flush_timeout() const noexcept {
        return fatal_flush_timeout_;
    }

private:
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

        const auto previous_pending = pending_.fetch_add(1U, std::memory_order_release);
        if (!push_record(lane, record)) {
            release_record(record);
            abandon_pending_record();
            record_dropped(lane);
            return false;
        }

        record_accepted(lane);
        if (previous_pending == 0U) {
            wake_cv_.notify_one();
        }
        return true;
    }

    class LogRecordPool {
        static constexpr std::uint32_t null_slot = std::numeric_limits<std::uint32_t>::max();
        static constexpr std::size_t slot_index_bits = sizeof(std::uint32_t) * 8U;
        static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;

        struct Slot {
            LogRecordPool *owner{nullptr};
            std::atomic<std::uint32_t> next{null_slot};
            detail::LogRecord record;
        };

    public:
        explicit LogRecordPool(std::size_t capacity)
            : capacity_(validate_capacity(capacity)), slots_(new Slot[capacity_]) {
            for (std::size_t i = 0; i < capacity_; ++i) {
                Slot &slot = slots_[i];
                slot.owner = this;
                slot.next.store(i + 1U < capacity_ ? static_cast<std::uint32_t>(i + 1U) : null_slot,
                                std::memory_order_relaxed);
                slot.record.set_pool_slot(&slot);
            }
            free_head_.store(pack_head(0U, 0U), std::memory_order_relaxed);
        }

        LogRecordPool(const LogRecordPool &) = delete;
        LogRecordPool &operator=(const LogRecordPool &) = delete;

        [[nodiscard]] detail::LogRecord *try_acquire(std::string_view message) noexcept {
            Slot *slot = try_pop();
            if (slot == nullptr) {
                return nullptr;
            }

            try {
                slot->record.reset(message);
            } catch (...) {
                release(slot);
                return nullptr;
            }
            return &slot->record;
        }

        static void release(detail::LogRecord *record) noexcept {
            auto *slot = static_cast<Slot *>(record->pool_slot());
            AF_ASSERT(slot != nullptr);
            slot->owner->release(slot);
        }

    private:
        [[nodiscard]] static std::size_t validate_capacity(std::size_t capacity) {
            if (capacity == 0U || capacity >= null_slot) {
                throw std::length_error("async log record pool capacity is out of range");
            }
            return capacity;
        }

        [[nodiscard]] static constexpr std::uint64_t pack_head(std::uint32_t index,
                                                               std::uint64_t version) noexcept {
            return (version << slot_index_bits) | static_cast<std::uint64_t>(index);
        }

        [[nodiscard]] static constexpr std::uint32_t head_index(std::uint64_t head) noexcept {
            return static_cast<std::uint32_t>(head & slot_index_mask);
        }

        [[nodiscard]] static constexpr std::uint64_t head_version(std::uint64_t head) noexcept {
            return head >> slot_index_bits;
        }

        [[nodiscard]] std::uint32_t slot_index(const Slot *slot) const noexcept {
            const auto index = static_cast<std::size_t>(slot - slots_.get());
            AF_ASSERT(index < capacity_);
            return static_cast<std::uint32_t>(index);
        }

        [[nodiscard]] Slot *try_pop() noexcept {
            std::uint64_t head = free_head_.load(std::memory_order_acquire);
            for (;;) {
                const std::uint32_t index = head_index(head);
                if (index == null_slot) {
                    return nullptr;
                }

                Slot &slot = slots_[index];
                const std::uint32_t next = slot.next.load(std::memory_order_relaxed);
                const std::uint64_t desired = pack_head(next, head_version(head) + 1U);
                if (free_head_.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                     std::memory_order_acquire)) {
                    return &slot;
                }
            }
        }

        void release(Slot *slot) noexcept {
            const std::uint32_t index = slot_index(slot);
            std::uint64_t head = free_head_.load(std::memory_order_relaxed);
            for (;;) {
                slot->next.store(head_index(head), std::memory_order_relaxed);
                const std::uint64_t desired = pack_head(index, head_version(head) + 1U);
                if (free_head_.compare_exchange_weak(head, desired, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                    return;
                }
            }
        }

        const std::size_t capacity_;
        std::unique_ptr<Slot[]> slots_;
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint64_t> free_head_{
            pack_head(null_slot, 0U)};
    };

    struct alignas(detail::hardware_cache_line_size) LogStatCounter {
        void add(std::uint64_t value) noexcept {
            count.fetch_add(value, std::memory_order_relaxed);
        }

        [[nodiscard]] std::uint64_t load() const noexcept {
            return count.load(std::memory_order_acquire);
        }

        std::atomic<std::uint64_t> count{0};
    };

    struct alignas(detail::hardware_cache_line_size) QueueShard {
        QueueShard(std::size_t queue_capacity, std::size_t record_capacity)
            : queue(queue_capacity), records(record_capacity) {}

        LogStatCounter accepted;
        LogStatCounter dropped;
        detail::BoundedMpscQueue<detail::LogRecord> queue;
        LogRecordPool records;
    };

    struct alignas(detail::hardware_cache_line_size) RuntimeLane {
        RuntimeLane(std::size_t queue_capacity, std::size_t record_capacity)
            : queue(queue_capacity), records(record_capacity) {}

        LogStatCounter accepted;
        LogStatCounter dropped;
        detail::BoundedSpscQueue<detail::LogRecord> queue;
        LogRecordPool records;
    };

    struct ProducerShardCache {
        const AsyncLogger *logger{nullptr};
        std::uint64_t token{0};
        QueueShard *shard{nullptr};
    };

    using QueueShardStorage = detail::ContiguousObjectStorage<QueueShard>;
    using RuntimeLaneStorage = detail::ContiguousObjectStorage<RuntimeLane>;

    [[nodiscard]] static std::size_t default_queue_shard_count() noexcept {
        const unsigned int hardware_threads = std::thread::hardware_concurrency();
        return hardware_threads == 0U
                   ? 1U
                   : detail::next_power_of_two(static_cast<std::size_t>(hardware_threads));
    }

    [[nodiscard]] static std::size_t normalize_queue_shard_count(std::size_t requested) noexcept {
        const std::size_t shard_count = requested == 0U ? default_queue_shard_count() : requested;
        return detail::next_power_of_two(shard_count == 0U ? 1U : shard_count);
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
        return std::max<std::size_t>(2U, (capacity + shard_count - 1U) / shard_count);
    }

    [[nodiscard]] static std::size_t
    queue_capacity_per_runtime_thread(std::size_t total_capacity,
                                      std::size_t thread_count) noexcept {
        if (thread_count == 0U) {
            return 0U;
        }
        const std::size_t capacity = total_capacity == 0U ? 1U : total_capacity;
        return std::max<std::size_t>(2U, (capacity + thread_count - 1U) / thread_count);
    }

    [[nodiscard]] static std::size_t record_capacity_per_shard(std::size_t queue_capacity,
                                                               std::size_t max_batch_size) {
        const std::size_t batch_capacity = max_batch_size == 0U ? 1U : max_batch_size;
        if (queue_capacity > std::numeric_limits<std::size_t>::max() - batch_capacity) {
            throw std::length_error("async log record pool capacity overflow");
        }
        return queue_capacity + batch_capacity;
    }

    [[nodiscard]] static QueueShardStorage make_queue_shards(std::size_t shard_count,
                                                             std::size_t capacity_per_shard,
                                                             std::size_t max_batch_size) {
        QueueShardStorage shards;
        shards.reserve_exact(shard_count);
        const std::size_t record_capacity =
            record_capacity_per_shard(capacity_per_shard, max_batch_size);
        for (std::size_t i = 0; i < shard_count; ++i) {
            shards.emplace_back(capacity_per_shard, record_capacity);
        }
        return shards;
    }

    [[nodiscard]] static RuntimeLaneStorage make_runtime_lanes(std::size_t thread_count,
                                                               std::size_t capacity_per_thread,
                                                               std::size_t max_batch_size) {
        RuntimeLaneStorage lanes;
        if (thread_count == 0U) {
            return lanes;
        }

        lanes.reserve_exact(thread_count);
        const std::size_t record_capacity =
            record_capacity_per_shard(capacity_per_thread, max_batch_size);
        for (std::size_t i = 0; i < thread_count; ++i) {
            lanes.emplace_back(capacity_per_thread, record_capacity);
        }
        return lanes;
    }

    [[nodiscard]] QueueShard &producer_shard() noexcept {
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

    static void release_record(detail::LogRecord *record) noexcept {
        LogRecordPool::release(record);
    }

    void abandon_pending_record() noexcept {
        const auto previous = pending_.fetch_sub(1U, std::memory_order_acq_rel);
        AF_ASSERT(previous != 0U);
        if (previous == 1U) {
            drained_cv_.notify_all();
        }
    }

    void worker_main() noexcept {
        std::vector<detail::LogRecord *> batch;
        batch.reserve(max_batch_size_);

        for (;;) {
            drain_available(batch);
            if (stopping_.load(std::memory_order_acquire) &&
                pending_.load(std::memory_order_acquire) == 0U) {
                break;
            }

            std::unique_lock lock(wait_mutex_);
            wake_cv_.wait_for(lock, flush_poll_interval_, [this] {
                return stopping_.load(std::memory_order_acquire) ||
                       pending_.load(std::memory_order_acquire) != 0U;
            });
        }

        drain_available(batch);
        flush_backends();
    }

    void drain_available(std::vector<detail::LogRecord *> &batch) noexcept {
        for (;;) {
            batch.clear();
            collect_batch(batch);

            if (batch.empty()) {
                return;
            }

            for (auto &backend : backends_) {
                backend->write_batch(
                    std::span<detail::LogRecord *const>(batch.data(), batch.size()));
            }

            for (detail::LogRecord *record : batch) {
                release_record(record);
            }

            const auto drained = batch.size();
            const auto previous = pending_.fetch_sub(drained, std::memory_order_acq_rel);
            AF_ASSERT(previous >= drained);
            if (previous == drained) {
                drained_cv_.notify_all();
            }
        }
    }

    void collect_batch(std::vector<detail::LogRecord *> &batch) noexcept {
        if (prefer_runtime_drain_) {
            collect_runtime_batch(batch);
            collect_shard_batch(batch);
        } else {
            collect_shard_batch(batch);
            collect_runtime_batch(batch);
        }
        prefer_runtime_drain_ = !prefer_runtime_drain_;
    }

    void collect_shard_batch(std::vector<detail::LogRecord *> &batch) noexcept {
        constexpr std::size_t max_queue_drain_count = 64;
        std::array<detail::LogRecord *, max_queue_drain_count> drained;
        std::size_t empty_visits = 0;
        while (batch.size() < max_batch_size_ && empty_visits < queue_shard_count_) {
            QueueShard &shard = *queue_shards_[next_drain_shard_];
            next_drain_shard_ = (next_drain_shard_ + 1U) & queue_shard_mask_;

            const std::size_t count = shard.queue.try_pop_many(
                drained.data(), std::min(drained.size(), max_batch_size_ - batch.size()));
            if (count == 0U) {
                ++empty_visits;
                continue;
            }

            empty_visits = 0;
            batch.insert(batch.end(), drained.data(), drained.data() + count);
        }
    }

    void collect_runtime_batch(std::vector<detail::LogRecord *> &batch) noexcept {
        if (runtime_thread_count_ == 0U) {
            return;
        }

        constexpr std::size_t max_queue_drain_count = 64;
        std::array<detail::LogRecord *, max_queue_drain_count> drained;
        std::size_t empty_visits = 0;
        while (batch.size() < max_batch_size_ && empty_visits < runtime_thread_count_) {
            RuntimeLane &lane = *runtime_lanes_[next_runtime_drain_thread_];
            ++next_runtime_drain_thread_;
            if (next_runtime_drain_thread_ == runtime_thread_count_) {
                next_runtime_drain_thread_ = 0U;
            }

            const std::size_t count = lane.queue.try_pop_many(
                drained.data(), std::min(drained.size(), max_batch_size_ - batch.size()));
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
            if (now >= deadline) {
                return false;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
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

    static inline std::atomic<std::uint64_t> next_cache_token_{1};

    const std::uint64_t cache_token_;
    const std::size_t queue_shard_count_;
    const std::size_t queue_shard_mask_;
    const std::size_t runtime_thread_count_;
    QueueShardStorage queue_shards_;
    RuntimeLaneStorage runtime_lanes_;
    const std::size_t max_batch_size_;
    const std::size_t overflow_spin_count_;
    const LogOverflowPolicy overflow_policy_;
    const std::chrono::milliseconds flush_poll_interval_;
    const std::chrono::milliseconds fatal_flush_timeout_;
    const std::string consumer_thread_name_;
    std::vector<std::unique_ptr<LogBackend>> backends_;

    alignas(detail::hardware_cache_line_size) std::atomic<bool> started_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> accepting_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> stopping_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> next_producer_shard_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> pending_{0};

    std::size_t next_drain_shard_{0};
    std::size_t next_runtime_drain_thread_{0};
    bool prefer_runtime_drain_{true};
    std::mutex wait_mutex_;
    std::condition_variable wake_cv_;
    std::condition_variable drained_cv_;
    std::thread worker_;
};

} // namespace af
