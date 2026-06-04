#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "af/detail/config.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/memory/contiguous_object_storage.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/timed_atomic_wait.hpp"

namespace af::detail {

template <typename BatchT>
[[nodiscard]] std::uint32_t runtime_log_batch_record_count(const BatchT &batch) noexcept {
    return batch.record_count;
}

template <typename BatchT> class RuntimeLogQueueState {
public:
    using Batch = BatchT;

    template <typename... BatchArgs>
    RuntimeLogQueueState(std::size_t batch_queue_capacity, std::size_t batch_record_capacity,
                         std::size_t batches_per_run, BatchArgs &&...batch_args)
        : max_batch_records(batch_record_capacity == 0U ? 1U : batch_record_capacity),
          max_batches_per_run(batches_per_run == 0U ? 1U : batches_per_run),
          ready_batches(batch_queue_capacity), free_batches(batch_queue_capacity) {
        reserve_batches(batch_queue_capacity, std::forward<BatchArgs>(batch_args)...);
    }

    RuntimeLogQueueState(const RuntimeLogQueueState &) = delete;
    RuntimeLogQueueState &operator=(const RuntimeLogQueueState &) = delete;

    [[nodiscard]] bool enqueue(std::span<LogRecord *const> records) noexcept {
        if (records.empty() || stopping.load(std::memory_order_relaxed)) {
            return false;
        }

        bool enqueued_any = false;
        std::size_t index = 0;
        while (index < records.size()) {
            while (index < records.size() && records[index]->message().empty()) {
                ++index;
            }
            if (index == records.size()) {
                return enqueued_any;
            }

            Batch *batch = acquire_producer_batch();
            if (batch == nullptr) {
                dropped_records.fetch_add(count_non_empty_records(records.subspan(index)),
                                          std::memory_order_relaxed);
                return enqueued_any;
            }

            batch->reset();
            const std::size_t begin = index;
            while (index < records.size()) {
                const std::string_view message = records[index]->message();
                if (message.empty()) {
                    ++index;
                    continue;
                }
                if (!batch->append(message, max_batch_records)) {
                    break;
                }
                ++index;
                if (runtime_log_batch_record_count(*batch) >= max_batch_records) {
                    break;
                }
            }

            if (batch->empty()) {
                stash_producer_batch(batch);
                if (index == begin) {
                    ++index;
                }
                continue;
            }

            const std::uint32_t queued_count = runtime_log_batch_record_count(*batch);
            pending_batches.fetch_add(1U, std::memory_order_relaxed);
            if (!ready_batches.try_push(batch)) [[unlikely]] {
                abandon_pending_batch();
                dropped_records.fetch_add(queued_count, std::memory_order_relaxed);
                stash_producer_batch(batch);
                return enqueued_any;
            }
            queued_records.fetch_add(queued_count, std::memory_order_relaxed);
            enqueued_any = true;
        }
        return enqueued_any;
    }

    [[nodiscard]] bool flush_until(std::chrono::steady_clock::time_point deadline) noexcept {
        if (pending_batches.load(std::memory_order_acquire) == 0U) {
            return true;
        }

        return wait_until_atomic_condition(
            pending_batches, deadline, [](std::size_t pending) noexcept { return pending == 0U; });
    }

    void complete_batch(Batch *batch) noexcept {
        if (batch == nullptr) {
            return;
        }
        recycle_batch(batch);
        abandon_pending_batch();
    }

    void recycle_batch(Batch *batch) noexcept {
        const bool recycled = free_batches.try_push(batch);
        AF_ASSERT(recycled);
        static_cast<void>(recycled);
    }

    void mark_finished() noexcept {
        finished.store(true, std::memory_order_release);
        finished.notify_all();
    }

    [[nodiscard]] bool
    wait_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        return wait_until_atomic_flag_true(finished, deadline);
    }

    const std::size_t max_batch_records;
    const std::size_t max_batches_per_run;
    BoundedMpscQueue<Batch> ready_batches;
    BoundedMpscQueue<Batch> free_batches;
    ContiguousObjectStorage<Batch> storage;
    CacheLineAtomic<std::uint64_t> queued_records{0};
    CacheLineAtomic<std::uint64_t> dropped_records{0};
    CacheLineAtomic<std::size_t> pending_batches{0};
    CacheLineAtomic<bool> wake_queued{false};
    CacheLineAtomic<bool> stopping{false};
    CacheLineAtomic<bool> finished{false};

private:
    [[nodiscard]] Batch *acquire_producer_batch() noexcept {
        if (producer_spare_batch_ != nullptr) {
            Batch *batch = producer_spare_batch_;
            producer_spare_batch_ = nullptr;
            return batch;
        }
        return free_batches.try_pop();
    }

    void stash_producer_batch(Batch *batch) noexcept {
        AF_ASSERT(producer_spare_batch_ == nullptr);
        producer_spare_batch_ = batch;
    }

    void abandon_pending_batch() noexcept {
        if (pending_batches.fetch_sub(1U, std::memory_order_release) == 1U) {
            pending_batches.notify_all();
        }
    }

    [[nodiscard]] static std::size_t
    count_non_empty_records(std::span<LogRecord *const> records) noexcept {
        std::size_t count = 0;
        for (const LogRecord *record : records) {
            if (record != nullptr && !record->message().empty()) {
                ++count;
            }
        }
        return count;
    }

    template <typename... BatchArgs>
    void reserve_batches(std::size_t queue_capacity, BatchArgs &&...batch_args) {
        const std::size_t capacity = queue_capacity == 0U ? 1U : queue_capacity;
        storage.reserve_exact(capacity);
        for (std::size_t i = 0; i < capacity; ++i) {
            Batch *ptr =
                &storage.emplace_back(max_batch_records, std::forward<BatchArgs>(batch_args)...);
            const bool ok = free_batches.try_push(ptr);
            AF_ASSERT(ok);
            static_cast<void>(ok);
        }
    }

    Batch *producer_spare_batch_{nullptr};
};

template <typename StateT, typename TaskHandleT, typename TaskStartedAtomicT>
[[nodiscard]] bool wake_runtime_log_task(StateT *state, TaskHandleT &task,
                                         TaskStartedAtomicT &task_started) noexcept {
    if (!task) {
        return false;
    }
    if (state->finished.load(std::memory_order_acquire)) {
        return true;
    }

    // wake_queued deduplicates producer wakeups, including the period where the
    // task is waiting for async IO. Avoid cross-thread IO-wait hints here: a
    // stale hint can otherwise strand records after the task has gone idle.
    bool wake_expected = false;
    if (!state->wake_queued.compare_exchange_strong(wake_expected, true, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        return true;
    }

    bool expected = false;
    if (task_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
        if (task->start(state)) {
            return true;
        }
        task_started.store(false, std::memory_order_release);
        state->wake_queued.store(false, std::memory_order_release);
        return false;
    }

    if (task->wake()) {
        return true;
    }
    state->wake_queued.store(false, std::memory_order_release);
    return false;
}

template <typename RuntimeT, typename StateT, typename TaskT> class RuntimeLogTaskBinding {
public:
    explicit RuntimeLogTaskBinding(std::unique_ptr<StateT> state)
        : state_(std::move(state)), task_(RuntimeT::template make_task<TaskT>()) {
        AF_ASSERT(state_ != nullptr);
        if (state_ == nullptr || RuntimeT::thread_index(state_->thread) >= RuntimeT::thread_count)
            [[unlikely]] {
            throw std::runtime_error("invalid runtime log backend thread");
        }
    }

    RuntimeLogTaskBinding(const RuntimeLogTaskBinding &) = delete;
    RuntimeLogTaskBinding &operator=(const RuntimeLogTaskBinding &) = delete;

    [[nodiscard]] StateT &state() noexcept {
        return *state_;
    }

    [[nodiscard]] const StateT &state() const noexcept {
        return *state_;
    }

    [[nodiscard]] bool enqueue_and_wake(std::span<LogRecord *const> records) noexcept {
        return state_->enqueue(records) && wake();
    }

    [[nodiscard]] bool wake() noexcept {
        return wake_runtime_log_task(state_.get(), task_, task_started_);
    }

    void stop_and_wait(std::chrono::steady_clock::time_point deadline) noexcept {
        state_->stopping.store(true, std::memory_order_relaxed);
        if (!task_started_.load(std::memory_order_acquire) &&
            state_->pending_batches.load(std::memory_order_acquire) == 0U) {
            state_->mark_finished();
            task_.reset();
            return;
        }

        if (wake()) {
            static_cast<void>(state_->wait_until_finished(deadline));
        }
        task_.reset();
    }

private:
    std::unique_ptr<StateT> state_;
    typename RuntimeT::template TaskHandle<TaskT> task_;
    CacheLineAtomic<bool> task_started_{false};
};

} // namespace af::detail
