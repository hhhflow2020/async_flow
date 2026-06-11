#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/memory/contiguous_object_storage.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/detail/runtime/timed_atomic_wait.hpp"
#include "af/runtime/runtime.hpp"
#include "af/span.hpp"

namespace af::detail {

class RuntimeBoundLogBatch {
public:
    explicit RuntimeBoundLogBatch(std::size_t max_records) {
        messages.reserve(max_records == 0U ? 1U : max_records);
    }

    void reset() noexcept {
        record_count = 0;
        messages.clear();
    }

    [[nodiscard]] bool append(std::string_view message, std::size_t max_records) {
        if (message.empty()) {
            return true;
        }
        if (record_count >= max_records) {
            return false;
        }
        messages.emplace_back(message.data(), message.size());
        ++record_count;
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return record_count == 0U;
    }

    std::uint32_t record_count{0};
    std::vector<std::string> messages;
};

struct RuntimeBoundLogBackendConfig {
    runtime *owner{nullptr};
    runtime::thread_index thread{runtime_invalid_thread_index};
    std::unique_ptr<log_backend> backend;
    std::size_t batch_queue_capacity{1024};
    std::size_t max_batch_records{64};
    std::size_t max_batches_per_run{64};
};

struct RuntimeBoundLogBackendStats {
    std::uint64_t queued_records{0};
    std::uint64_t written_records{0};
    std::uint64_t dropped_records{0};
    std::uint64_t flushes{0};
};

class RuntimeBoundLogBackend final : public log_backend, public runtime_service_task {
public:
    using Batch = RuntimeBoundLogBatch;

    explicit RuntimeBoundLogBackend(RuntimeBoundLogBackendConfig config)
        : owner_(config.owner), thread_(config.thread), backend_(std::move(config.backend)),
          max_batch_records_(normalize_max_batch_records(config.max_batch_records)),
          max_batches_per_run_(config.max_batches_per_run == 0U ? 1U : config.max_batches_per_run),
          ready_batches_(normalize_batch_queue_capacity(config.batch_queue_capacity)),
          free_batches_(normalize_batch_queue_capacity(config.batch_queue_capacity)),
          scratch_records_(std::make_unique<log_record[]>(max_batch_records_)) {
        AF_ASSERT(owner_ != nullptr);
        AF_ASSERT(backend_ != nullptr);
        if (owner_ == nullptr || backend_ == nullptr || !owner_->valid_thread(thread_)) {
            throw std::runtime_error("invalid runtime-bound log backend");
        }
        reserve_batches(normalize_batch_queue_capacity(config.batch_queue_capacity));
        scratch_record_ptrs_.reserve(max_batch_records_);
        if (!register_on_owner_thread()) {
            throw std::runtime_error("failed to register runtime-bound log backend");
        }
    }

    RuntimeBoundLogBackend(const RuntimeBoundLogBackend &) = delete;
    RuntimeBoundLogBackend &operator=(const RuntimeBoundLogBackend &) = delete;

    ~RuntimeBoundLogBackend() override {
        shutdown();
    }

    void write_batch(af::span<log_record *const> records) noexcept override {
        static_cast<void>(enqueue(records));
    }

    void flush() noexcept override {
        static_cast<void>(flush(std::chrono::seconds(5)));
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept override {
        const std::uint64_t target = flush_requests_.fetch_add(1U, std::memory_order_relaxed) + 1U;
        if (!wake()) {
            return false;
        }
        return wait_for_flush(target, std::chrono::steady_clock::now() + timeout);
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        stopping_.store(true, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        const std::uint64_t target = flush_requests_.fetch_add(1U, std::memory_order_relaxed) + 1U;
        static_cast<void>(wake());
        static_cast<void>(wait_for_flush(target, deadline));
        static_cast<void>(wait_until_finished(deadline));
        if (registered_.load(std::memory_order_acquire)) {
            static_cast<void>(unregister_on_owner_thread());
        }
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        bool did_work = false;
        std::size_t drained_batches = 0;
        const std::size_t effective_budget = effective_batch_budget(budget);
        while (drained_batches < effective_budget) {
            Batch *batch = ready_batches_.try_pop();
            if (batch == nullptr) {
                break;
            }
            write_batch_on_owner(*batch);
            recycle_batch(batch);
            complete_pending_batch();
            ++drained_batches;
            did_work = true;
        }

        if (pending_batches_.load(std::memory_order_acquire) == 0U &&
            completed_flushes_.load(std::memory_order_acquire) <
                flush_requests_.load(std::memory_order_relaxed)) {
            backend_->flush();
            flushes_.fetch_add(1U, std::memory_order_relaxed);
            const std::uint64_t requested = flush_requests_.load(std::memory_order_relaxed);
            completed_flushes_.store(requested, std::memory_order_release);
            completed_flushes_.notify_all();
            did_work = true;
        }

        if (stopping_.load(std::memory_order_acquire) &&
            pending_batches_.load(std::memory_order_acquire) == 0U &&
            completed_flushes_.load(std::memory_order_acquire) >=
                flush_requests_.load(std::memory_order_relaxed)) {
            backend_->shutdown();
            mark_finished();
            return did_work;
        }

        return did_work || pending_batches_.load(std::memory_order_acquire) != 0U;
    }

    [[nodiscard]] RuntimeBoundLogBackendStats stats() const noexcept {
        return RuntimeBoundLogBackendStats{
            queued_records_.load(std::memory_order_relaxed),
            written_records_.load(std::memory_order_relaxed),
            dropped_records_.load(std::memory_order_relaxed),
            flushes_.load(std::memory_order_relaxed),
        };
    }

private:
    enum class ControlOperation : std::uint8_t {
        Register,
        Unregister,
    };

    struct ControlCompletion {
        std::atomic<bool> done{false};
        std::atomic<bool> ok{false};
    };

    [[nodiscard]] static std::size_t normalize_max_batch_records(std::size_t requested) noexcept {
        constexpr std::size_t max_supported_records = 1024;
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_supported_records);
    }

    [[nodiscard]] static std::size_t
    normalize_batch_queue_capacity(std::size_t requested) noexcept {
        return requested == 0U ? 1U : requested;
    }

    void reserve_batches(std::size_t capacity) {
        storage_.reserve_exact(capacity);
        for (std::size_t i = 0; i < capacity; ++i) {
            Batch *batch = &storage_.emplace_back(max_batch_records_);
            const bool ok = free_batches_.try_push(batch);
            AF_ASSERT(ok);
            static_cast<void>(ok);
        }
    }

    [[nodiscard]] bool enqueue(af::span<log_record *const> records) noexcept {
        if (records.empty() || stopping_.load(std::memory_order_acquire)) {
            return false;
        }

        bool enqueued_any = false;
        std::size_t index = 0;
        while (index < records.size()) {
            while (index < records.size() &&
                   (records[index] == nullptr || records[index]->message().empty())) {
                ++index;
            }
            if (index == records.size()) {
                return enqueued_any;
            }

            Batch *batch = acquire_batch();
            if (batch == nullptr) {
                dropped_records_.fetch_add(count_non_empty_records(records.subspan(index)),
                                           std::memory_order_relaxed);
                return enqueued_any;
            }

            batch->reset();
            const std::size_t begin = index;
            while (index < records.size()) {
                log_record *record = records[index];
                const std::string_view message =
                    record == nullptr ? std::string_view{} : record->message();
                if (message.empty()) {
                    ++index;
                    continue;
                }
                if (!batch->append(message, max_batch_records_)) {
                    break;
                }
                ++index;
                if (batch->record_count >= max_batch_records_) {
                    break;
                }
            }

            if (batch->empty()) {
                stash_batch(batch);
                if (index == begin) {
                    ++index;
                }
                continue;
            }

            const std::uint32_t queued_count = batch->record_count;
            pending_batches_.fetch_add(1U, std::memory_order_relaxed);
            if (!ready_batches_.try_push(batch)) [[unlikely]] {
                complete_pending_batch();
                dropped_records_.fetch_add(queued_count, std::memory_order_relaxed);
                stash_batch(batch);
                return enqueued_any;
            }
            queued_records_.fetch_add(queued_count, std::memory_order_relaxed);
            enqueued_any = true;
        }

        if (enqueued_any) {
            static_cast<void>(wake());
        }
        return enqueued_any;
    }

    [[nodiscard]] Batch *acquire_batch() noexcept {
        if (producer_spare_batch_ != nullptr) {
            Batch *batch = producer_spare_batch_;
            producer_spare_batch_ = nullptr;
            return batch;
        }
        return free_batches_.try_pop();
    }

    void stash_batch(Batch *batch) noexcept {
        AF_ASSERT(producer_spare_batch_ == nullptr);
        producer_spare_batch_ = batch;
    }

    void recycle_batch(Batch *batch) noexcept {
        batch->reset();
        const bool ok = free_batches_.try_push(batch);
        AF_ASSERT(ok);
        static_cast<void>(ok);
    }

    void complete_pending_batch() noexcept {
        if (pending_batches_.fetch_sub(1U, std::memory_order_release) == 1U) {
            pending_batches_.notify_all();
        }
    }

    [[nodiscard]] static std::size_t
    count_non_empty_records(af::span<log_record *const> records) noexcept {
        std::size_t count = 0;
        for (const log_record *record : records) {
            if (record != nullptr && !record->message().empty()) {
                ++count;
            }
        }
        return count;
    }

    void write_batch_on_owner(const Batch &batch) noexcept {
        scratch_record_ptrs_.clear();
        for (std::size_t i = 0; i < batch.messages.size(); ++i) {
            scratch_records_[i].reset(batch.messages[i]);
            scratch_record_ptrs_.push_back(&scratch_records_[i]);
        }
        if (scratch_record_ptrs_.empty()) {
            return;
        }
        backend_->write_batch(
            af::span<log_record *const>(scratch_record_ptrs_.data(), scratch_record_ptrs_.size()));
        written_records_.fetch_add(scratch_record_ptrs_.size(), std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t effective_batch_budget(std::size_t service_budget) const noexcept {
        if (service_budget == 0U || service_budget > max_batches_per_run_) {
            return max_batches_per_run_;
        }
        return service_budget;
    }

    [[nodiscard]] bool wake() noexcept {
        return registered_.load(std::memory_order_acquire) && owner_->wake_service_tasks(thread_);
    }

    [[nodiscard]] bool is_owner_runtime_thread() const noexcept {
        return runtime::current() == owner_ && runtime::current_thread_index() == thread_;
    }

    [[nodiscard]] bool wait_for_flush(std::uint64_t target,
                                      std::chrono::steady_clock::time_point deadline) noexcept {
        if (is_owner_runtime_thread()) {
            while (completed_flushes_.load(std::memory_order_acquire) < target &&
                   std::chrono::steady_clock::now() < deadline) {
                if (!run_service(max_batches_per_run_)) {
                    break;
                }
            }
        }
        return wait_until_atomic_condition(
            completed_flushes_, deadline,
            [target](std::uint64_t completed) noexcept { return completed >= target; });
    }

    [[nodiscard]] bool
    wait_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        if (is_owner_runtime_thread()) {
            while (!finished_.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < deadline) {
                if (!run_service(max_batches_per_run_)) {
                    break;
                }
            }
        }
        return wait_until_atomic_flag_true(finished_, deadline);
    }

    void mark_finished() noexcept {
        bool expected = false;
        if (finished_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            finished_.notify_all();
        }
    }

    [[nodiscard]] bool register_on_owner_thread() noexcept {
        return run_control_and_wait(ControlOperation::Register);
    }

    [[nodiscard]] bool unregister_on_owner_thread() noexcept {
        return run_control_and_wait(ControlOperation::Unregister);
    }

    [[nodiscard]] bool run_control(ControlOperation operation) noexcept {
        switch (operation) {
        case ControlOperation::Register: {
            const bool ok = owner_->register_service_task(thread_, this);
            if (ok) {
                registered_.store(true, std::memory_order_release);
            }
            return ok;
        }
        case ControlOperation::Unregister: {
            const bool ok = owner_->unregister_service_task(thread_, this);
            if (ok) {
                registered_.store(false, std::memory_order_release);
            }
            return ok;
        }
        }
        return false;
    }

    [[nodiscard]] bool run_control_and_wait(ControlOperation operation) noexcept {
        if (is_owner_runtime_thread()) {
            return run_control(operation);
        }

        ControlCompletion completion;
        if (!owner_->post(thread_, [this, operation, &completion](runtime &) noexcept {
                const bool ok = run_control(operation);
                completion.ok.store(ok, std::memory_order_release);
                completion.done.store(true, std::memory_order_release);
            })) {
            return false;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_until_atomic_flag_true(completion.done, deadline)) {
            return false;
        }
        return completion.ok.load(std::memory_order_acquire);
    }

    runtime *owner_{nullptr};
    runtime::thread_index thread_{runtime_invalid_thread_index};
    std::unique_ptr<log_backend> backend_;
    const std::size_t max_batch_records_;
    const std::size_t max_batches_per_run_;
    BoundedMpscQueue<Batch> ready_batches_;
    BoundedMpscQueue<Batch> free_batches_;
    ContiguousObjectStorage<Batch> storage_;
    std::unique_ptr<log_record[]> scratch_records_;
    std::vector<log_record *> scratch_record_ptrs_;
    Batch *producer_spare_batch_{nullptr};
    CacheLineAtomic<std::uint64_t> queued_records_{0};
    CacheLineAtomic<std::uint64_t> written_records_{0};
    CacheLineAtomic<std::uint64_t> dropped_records_{0};
    CacheLineAtomic<std::uint64_t> flushes_{0};
    CacheLineAtomic<std::size_t> pending_batches_{0};
    CacheLineAtomic<std::uint64_t> flush_requests_{0};
    CacheLineAtomic<std::uint64_t> completed_flushes_{0};
    CacheLineAtomic<bool> registered_{false};
    CacheLineAtomic<bool> stopping_{false};
    CacheLineAtomic<bool> shutdown_started_{false};
    CacheLineAtomic<bool> finished_{false};
};

[[nodiscard]] inline std::unique_ptr<log_backend>
make_runtime_bound_log_backend(RuntimeBoundLogBackendConfig config) {
    return std::make_unique<RuntimeBoundLogBackend>(std::move(config));
}

} // namespace af::detail
