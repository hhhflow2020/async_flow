#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_record.hpp"
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
        while (pending_batches.load(std::memory_order_acquire) != 0U) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
        return true;
    }

    void complete_batch(Batch *batch) noexcept {
        if (batch == nullptr) {
            return;
        }
        recycle_batch(batch);
        if (pending_batches.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
            pending_batches.notify_all();
        }
    }

    void recycle_batch(Batch *batch) noexcept {
        const bool recycled = free_batches.try_push(batch);
        AF_ASSERT(recycled);
        static_cast<void>(recycled);
    }

    const std::size_t max_batch_records;
    const std::size_t max_batches_per_run;
    BoundedSpscQueue<Batch> ready_batches;
    BoundedSpscQueue<Batch> free_batches;
    std::vector<std::unique_ptr<Batch>> storage;
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
        storage.reserve(capacity);
        for (std::size_t i = 0; i < capacity; ++i) {
            auto batch =
                std::make_unique<Batch>(max_batch_records, std::forward<BatchArgs>(batch_args)...);
            Batch *ptr = batch.get();
            storage.push_back(std::move(batch));
            const bool ok = free_batches.try_push(ptr);
            AF_ASSERT(ok);
            static_cast<void>(ok);
        }
    }
};

} // namespace af::detail
