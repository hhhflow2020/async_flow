#pragma once

#include <cstdint>
#include <utility>

#include "absl/container/flat_hash_map.h"

namespace af {

enum class BatchSubmitStatus {
    Submitted,
    Buffered,
    Duplicate,
};

enum class OrderedBatchFailureAction : std::uint8_t {
    Retry,
    Skip,
    Stop,
};

struct OrderedBatchRetrySkipOptions {
    std::uint32_t max_retries{0};
    bool skip_after_retries{true};
};

struct OrderedBatchFailureDecision {
    OrderedBatchFailureAction action{OrderedBatchFailureAction::Stop};
    std::uint32_t failure_count{0};

    [[nodiscard]] bool should_retry() const noexcept {
        return action == OrderedBatchFailureAction::Retry;
    }

    [[nodiscard]] bool should_skip() const noexcept {
        return action == OrderedBatchFailureAction::Skip;
    }

    [[nodiscard]] bool should_stop() const noexcept {
        return action == OrderedBatchFailureAction::Stop;
    }
};

template <typename BatchId = std::uint64_t> class OrderedBatchRetrySkipPolicy {
public:
    explicit OrderedBatchRetrySkipPolicy(OrderedBatchRetrySkipOptions options = {})
        : options_(options) {}

    [[nodiscard]] OrderedBatchFailureDecision record_failure(BatchId batch_id) {
        const std::uint32_t failure_count = ++failures_[batch_id];
        if (failure_count <= options_.max_retries) {
            return {OrderedBatchFailureAction::Retry, failure_count};
        }
        if (options_.skip_after_retries) {
            return {OrderedBatchFailureAction::Skip, failure_count};
        }
        return {OrderedBatchFailureAction::Stop, failure_count};
    }

    void record_success(BatchId batch_id) {
        failures_.erase(batch_id);
    }

    void reset(BatchId batch_id) {
        failures_.erase(batch_id);
    }

    void clear() noexcept {
        failures_.clear();
    }

    [[nodiscard]] std::uint32_t failure_count(BatchId batch_id) const {
        const auto it = failures_.find(batch_id);
        return it == failures_.end() ? 0U : it->second;
    }

private:
    OrderedBatchRetrySkipOptions options_;
    absl::flat_hash_map<BatchId, std::uint32_t> failures_;
};

template <typename Batch> class BatchSequencer {
public:
    explicit BatchSequencer(std::uint64_t first_batch_id = 1) : next_batch_id_(first_batch_id) {}

    [[nodiscard]] std::uint64_t next_batch_id() const noexcept {
        return next_batch_id_;
    }

    template <typename SubmitFn>
    BatchSubmitStatus submit(std::uint64_t batch_id, Batch batch, SubmitFn &&submit_fn) {
        if (batch_id < next_batch_id_) {
            return BatchSubmitStatus::Duplicate;
        }

        if (batch_id > next_batch_id_) {
            const auto [_, inserted] = pending_.emplace(batch_id, std::move(batch));
            if (!inserted) {
                return BatchSubmitStatus::Duplicate;
            }
            return BatchSubmitStatus::Buffered;
        }

        submit_ready(std::move(batch), submit_fn);
        drain_ready(submit_fn);
        return BatchSubmitStatus::Submitted;
    }

private:
    template <typename SubmitFn> void submit_ready(Batch batch, SubmitFn &submit_fn) {
        submit_fn(std::move(batch));
        ++next_batch_id_;
    }

    template <typename SubmitFn> void drain_ready(SubmitFn &submit_fn) {
        for (;;) {
            auto it = pending_.find(next_batch_id_);
            if (it == pending_.end()) {
                return;
            }

            Batch batch = std::move(it->second);
            pending_.erase(it);
            submit_ready(std::move(batch), submit_fn);
        }
    }

    std::uint64_t next_batch_id_;
    absl::flat_hash_map<std::uint64_t, Batch> pending_;
};

using batch_submit_status = BatchSubmitStatus;
using ordered_batch_failure_action = OrderedBatchFailureAction;
using ordered_batch_retry_skip_options = OrderedBatchRetrySkipOptions;
using ordered_batch_failure_decision = OrderedBatchFailureDecision;

template <typename BatchId = std::uint64_t>
using ordered_batch_retry_skip_policy = OrderedBatchRetrySkipPolicy<BatchId>;

template <typename Batch> using batch_sequencer = BatchSequencer<Batch>;

} // namespace af
