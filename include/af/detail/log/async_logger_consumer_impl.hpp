#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <vector>

#include "af/span.hpp"

namespace af {

inline async_logger::~async_logger() {
    shutdown();
}

inline bool async_logger::flush(std::chrono::milliseconds timeout) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    if (!drain_waiter_.wait_until_drained(pending_, deadline, flush_poll_interval_,
                                          [this] { notify_consumer(); })) {
        return false;
    }
    return flush_backends_until(deadline);
}

inline bool
async_logger::start_bound_consumer(detail::AsyncLogConsumerWakeTarget &target) noexcept {
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

inline void async_logger::stop_bound_consumer_admission() noexcept {
    accepting_.store(false, std::memory_order_release);
    stopping_.store(true, std::memory_order_relaxed);
}

inline void async_logger::finish_bound_consumer_shutdown() noexcept {
    consumer_wake_target_.store(nullptr, std::memory_order_release);
    shutdown_backends();
    started_.store(false, std::memory_order_release);
}

inline bool async_logger::consumer_stop_requested() const noexcept {
    return stopping_.load(std::memory_order_relaxed);
}

inline std::size_t async_logger::pending_record_count() const noexcept {
    return pending_.load(std::memory_order_acquire);
}

inline std::size_t async_logger::ready_record_count() const noexcept {
    return ready_.load(std::memory_order_acquire);
}

inline std::size_t async_logger::max_batch_size() const noexcept {
    return max_batch_size_;
}

inline void async_logger::shutdown() noexcept {
    const bool was_started = started_.exchange(false, std::memory_order_acq_rel);
    accepting_.store(false, std::memory_order_release);
    stopping_.store(true, std::memory_order_relaxed);
    notify_consumer();
    if (was_started) {
        shutdown_backends();
    }
}

inline void async_logger::abandon_pending_record() noexcept {
    const auto previous = pending_.fetch_sub(1U, std::memory_order_relaxed);
    AF_ASSERT(previous != 0U);
    if (previous == 1U) {
        drain_waiter_.notify_drained();
        notify_consumer();
    }
}

inline bool async_logger::drain_some(std::vector<detail::log_record *> &batch,
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
            backend->write_batch(af::span<detail::log_record *const>(batch.data(), batch.size()));
        }

        const auto drained = batch.size();
        detail::release_async_log_records(
            af::span<detail::log_record *const>(batch.data(), drained));
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

inline void async_logger::collect_batch(std::vector<detail::log_record *> &batch,
                                        std::size_t max_records) noexcept {
    if (ordering_ == log_ordering::ordered) [[likely]] {
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

inline void async_logger::collect_ordered_batch(std::vector<detail::log_record *> &batch,
                                                std::size_t max_records) noexcept {
    AF_ASSERT(ordered_queue_ != nullptr);
    constexpr std::size_t max_queue_drain_count = 64;
    std::array<detail::log_record *, max_queue_drain_count> drained;
    while (batch.size() < max_records) {
        const std::size_t count = ordered_queue_->queue.try_pop_many(
            drained.data(), std::min(drained.size(), max_records - batch.size()));
        if (count == 0U) {
            return;
        }
        batch.insert(batch.end(), drained.data(), drained.data() + count);
    }
}

inline void async_logger::collect_shard_batch(std::vector<detail::log_record *> &batch,
                                              std::size_t max_records) noexcept {
    constexpr std::size_t max_queue_drain_count = 64;
    std::array<detail::log_record *, max_queue_drain_count> drained;
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

inline void async_logger::collect_runtime_batch(std::vector<detail::log_record *> &batch,
                                                std::size_t max_records) noexcept {
    if (runtime_thread_count_ == 0U) {
        return;
    }

    constexpr std::size_t max_queue_drain_count = 64;
    std::array<detail::log_record *, max_queue_drain_count> drained;
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

inline void async_logger::flush_backends() noexcept {
    for (auto &backend : backends_) {
        backend->flush();
    }
}

inline bool
async_logger::flush_backends_until(std::chrono::steady_clock::time_point deadline) noexcept {
    for (auto &backend : backends_) {
        const auto now = std::chrono::steady_clock::now();
        const auto remaining =
            now >= deadline ? std::chrono::milliseconds(0)
                            : std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        if (!backend->flush(remaining)) {
            return false;
        }
    }
    return true;
}

inline void async_logger::shutdown_backends() noexcept {
    for (auto &backend : backends_) {
        backend->shutdown();
    }
}

inline void async_logger::notify_consumer() noexcept {
    detail::AsyncLogConsumerWakeTarget *target =
        consumer_wake_target_.load(std::memory_order_acquire);
    if (target != nullptr) {
        static_cast<void>(target->wake_async_log_consumer());
    }
}

} // namespace af
