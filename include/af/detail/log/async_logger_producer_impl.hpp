#pragma once

#include <cstddef>
#include <string_view>

namespace af {

inline bool async_logger::try_log(std::string_view message) noexcept {
    if (ordering_ == log_ordering::ordered) [[likely]] {
        return try_log_ordered(message);
    }

    detail::AsyncLogQueueShard &shard = producer_shard();
    return try_log_on_lane(shard, message);
}

inline bool async_logger::try_log_from_runtime_thread(std::uint16_t thread_index,
                                                      std::string_view message) noexcept {
    if (ordering_ == log_ordering::ordered) [[likely]] {
        return try_log(message);
    }

    if (thread_index >= runtime_thread_count_) [[unlikely]] {
        return try_log(message);
    }

    detail::AsyncLogRuntimeLane &lane = *runtime_lanes_[thread_index];
    return try_log_on_lane(lane, message);
}

template <typename Lane>
inline bool async_logger::try_log_on_lane(Lane &lane, std::string_view message) noexcept {
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

inline detail::AsyncLogProducerShard &async_logger::ordered_producer_shard() noexcept {
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

inline detail::AsyncLogQueueShard &async_logger::producer_shard() noexcept {
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

inline bool async_logger::try_log_ordered(std::string_view message) noexcept {
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

template <typename Lane> inline void async_logger::record_accepted(Lane &lane) noexcept {
    lane.accepted.add(1U);
}

template <typename Lane> inline void async_logger::record_dropped(Lane &lane) noexcept {
    lane.dropped.add(1U);
}

template <typename Lane>
inline detail::LogRecord *async_logger::acquire_record(Lane &lane,
                                                       std::string_view message) noexcept {
    if (overflow_policy_ == log_overflow_policy::drop_newest) {
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
inline bool async_logger::push_record(Lane &lane, detail::LogRecord *record) noexcept {
    if (overflow_policy_ == log_overflow_policy::drop_newest) {
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

inline bool async_logger::push_ordered_record(detail::LogRecord *record) noexcept {
    AF_ASSERT(ordered_queue_ != nullptr);
    if (overflow_policy_ == log_overflow_policy::drop_newest) {
        return accepting_.load(std::memory_order_acquire) && ordered_queue_->queue.try_push(record);
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

inline void async_logger::release_record(detail::LogRecord *record) noexcept {
    detail::release_async_log_record(record);
}

inline void async_logger::release_unpublished_record(detail::AsyncLogQueueShard &,
                                                     detail::LogRecord *record) noexcept {
    release_record(record);
}

inline void async_logger::release_unpublished_record(detail::AsyncLogProducerShard &,
                                                     detail::LogRecord *record) noexcept {
    release_record(record);
}

inline void async_logger::release_unpublished_record(detail::AsyncLogRuntimeLane &lane,
                                                     detail::LogRecord *record) noexcept {
    static_cast<void>(lane);
    release_record(record);
}

} // namespace af
