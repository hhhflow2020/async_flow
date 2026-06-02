#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <utility>

#include "af/detail/config.hpp"
#include "af/detail/log/log_record.hpp"
#include "af/detail/memory/contiguous_object_storage.hpp"
#include "af/detail/queue/bounded_spsc_queue.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"

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
        if (records.empty() || stopping.load(std::memory_order_acquire)) {
            return false;
        }

        bool enqueued_any = false;
        std::size_t index = 0;
        while (index < records.size()) {
            Batch *batch = free_batches.try_pop();
            if (batch == nullptr) {
                dropped_records.fetch_add(records.size() - index, std::memory_order_relaxed);
                return enqueued_any;
            }

            batch->reset();
            const std::size_t begin = index;
            while (index < records.size()) {
                const std::string_view message = records[index]->message();
                if (!batch->append(message, max_batch_records)) {
                    break;
                }
                ++index;
                if (runtime_log_batch_record_count(*batch) >= max_batch_records) {
                    break;
                }
            }

            if (batch->empty()) {
                recycle_batch(batch);
                if (index == begin) {
                    ++index;
                }
                continue;
            }

            queued_records.fetch_add(runtime_log_batch_record_count(*batch),
                                     std::memory_order_relaxed);
            pending_batches.fetch_add(1U, std::memory_order_acq_rel);
            if (!ready_batches.try_push(batch)) [[unlikely]] {
                complete_batch(batch);
                dropped_records.fetch_add(index - begin, std::memory_order_relaxed);
                return enqueued_any;
            }
            enqueued_any = true;
        }
        return enqueued_any;
    }

    [[nodiscard]] bool flush_until(std::chrono::steady_clock::time_point deadline) noexcept {
        if (pending_batches.load(std::memory_order_acquire) == 0U) {
            return true;
        }

        std::unique_lock lock(pending_mutex_);
        return pending_cv_.wait_until(lock, deadline, [this] {
            return pending_batches.load(std::memory_order_acquire) == 0U;
        });
    }

    void complete_batch(Batch *batch) noexcept {
        if (batch == nullptr) {
            return;
        }
        recycle_batch(batch);
        if (pending_batches.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
            pending_batches.notify_all();
            pending_cv_.notify_all();
        }
    }

    void recycle_batch(Batch *batch) noexcept {
        const bool recycled = free_batches.try_push(batch);
        AF_ASSERT(recycled);
        static_cast<void>(recycled);
    }

    void mark_finished() noexcept {
        finished.store(true, std::memory_order_release);
        finished.notify_all();
        finished_cv_.notify_all();
    }

    [[nodiscard]] bool
    wait_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        if (finished.load(std::memory_order_acquire)) {
            return true;
        }

        std::unique_lock lock(finished_mutex_);
        return finished_cv_.wait_until(lock, deadline,
                                       [this] { return finished.load(std::memory_order_acquire); });
    }

    const std::size_t max_batch_records;
    const std::size_t max_batches_per_run;
    BoundedSpscQueue<Batch> ready_batches;
    BoundedSpscQueue<Batch> free_batches;
    ContiguousObjectStorage<Batch> storage;
    CacheLineAtomic<std::uint64_t> queued_records{0};
    CacheLineAtomic<std::uint64_t> dropped_records{0};
    CacheLineAtomic<std::size_t> pending_batches{0};
    CacheLineAtomic<bool> wake_queued{false};
    CacheLineAtomic<bool> stopping{false};
    CacheLineAtomic<bool> finished{false};

private:
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

    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    std::mutex finished_mutex_;
    std::condition_variable finished_cv_;
};

template <typename StateT, typename TaskHandleT>
[[nodiscard]] bool wake_runtime_log_task(StateT *state, TaskHandleT &task,
                                         std::atomic<bool> &task_started,
                                         bool skip_when_io_waiting) noexcept {
    if (!task) {
        return false;
    }
    if (state->finished.load(std::memory_order_acquire)) {
        return true;
    }
    if (skip_when_io_waiting && state->io_waiting.load(std::memory_order_acquire) &&
        !state->stopping.load(std::memory_order_acquire)) {
        return true;
    }

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
    }

    RuntimeLogTaskBinding(const RuntimeLogTaskBinding &) = delete;
    RuntimeLogTaskBinding &operator=(const RuntimeLogTaskBinding &) = delete;

    [[nodiscard]] StateT &state() noexcept {
        return *state_;
    }

    [[nodiscard]] const StateT &state() const noexcept {
        return *state_;
    }

    [[nodiscard]] bool enqueue_and_wake(std::span<LogRecord *const> records,
                                        bool skip_when_io_waiting) noexcept {
        return state_->enqueue(records) && wake(skip_when_io_waiting);
    }

    [[nodiscard]] bool wake(bool skip_when_io_waiting) noexcept {
        return wake_runtime_log_task(state_.get(), task_, task_started_, skip_when_io_waiting);
    }

    void stop_and_wait(std::chrono::steady_clock::time_point deadline,
                       bool skip_when_io_waiting) noexcept {
        state_->stopping.store(true, std::memory_order_release);
        if (!task_started_.load(std::memory_order_acquire) &&
            state_->pending_batches.load(std::memory_order_acquire) == 0U) {
            state_->mark_finished();
            task_.reset();
            return;
        }

        if (wake(skip_when_io_waiting)) {
            static_cast<void>(state_->wait_until_finished(deadline));
        }
        task_.reset();
    }

private:
    std::unique_ptr<StateT> state_;
    typename RuntimeT::template TaskHandle<TaskT> task_;
    std::atomic<bool> task_started_{false};
};

} // namespace af::detail
