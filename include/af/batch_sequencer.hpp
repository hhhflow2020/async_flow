#pragma once

#include <cstdint>
#include <utility>

#include "absl/container/flat_hash_map.h"

namespace af {

enum class batch_submit_status {
    submitted,
    buffered,
    duplicate,
};

enum class ordered_batch_failure_action : std::uint8_t {
    retry,
    skip,
    stop,
};

struct ordered_batch_retry_skip_options {
    std::uint32_t max_retries{0};
    bool skip_after_retries{true};
};

struct ordered_batch_failure_decision {
    ordered_batch_failure_action action{ordered_batch_failure_action::stop};
    std::uint32_t failure_count{0};

    [[nodiscard]] bool should_retry() const noexcept {
        return action == ordered_batch_failure_action::retry;
    }

    [[nodiscard]] bool should_skip() const noexcept {
        return action == ordered_batch_failure_action::skip;
    }

    [[nodiscard]] bool should_stop() const noexcept {
        return action == ordered_batch_failure_action::stop;
    }
};

using BatchSubmitStatus = batch_submit_status;
using OrderedBatchFailureAction = ordered_batch_failure_action;
using OrderedBatchRetrySkipOptions = ordered_batch_retry_skip_options;
using OrderedBatchFailureDecision = ordered_batch_failure_decision;

template <typename BatchId = std::uint64_t> class ordered_batch_retry_skip_policy {
public:
    explicit ordered_batch_retry_skip_policy(ordered_batch_retry_skip_options options = {})
        : options_(options) {}

    [[nodiscard]] ordered_batch_failure_decision record_failure(BatchId batch_id) {
        const std::uint32_t failure_count = ++failures_[batch_id];
        if (failure_count <= options_.max_retries) {
            return {ordered_batch_failure_action::retry, failure_count};
        }
        if (options_.skip_after_retries) {
            return {ordered_batch_failure_action::skip, failure_count};
        }
        return {ordered_batch_failure_action::stop, failure_count};
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
    ordered_batch_retry_skip_options options_;
    absl::flat_hash_map<BatchId, std::uint32_t> failures_;
};

template <typename BatchId = std::uint64_t>
using OrderedBatchRetrySkipPolicy = ordered_batch_retry_skip_policy<BatchId>;

template <typename Batch> class batch_sequencer {
public:
    explicit batch_sequencer(std::uint64_t first_batch_id = 1) : next_batch_id_(first_batch_id) {}

    [[nodiscard]] std::uint64_t next_batch_id() const noexcept {
        return next_batch_id_;
    }

    template <typename SubmitFn>
    batch_submit_status submit(std::uint64_t batch_id, Batch batch, SubmitFn &&submit_fn) {
        if (batch_id < next_batch_id_) {
            return batch_submit_status::duplicate;
        }

        if (batch_id > next_batch_id_) {
            const auto [_, inserted] = pending_.emplace(batch_id, std::move(batch));
            if (!inserted) {
                return batch_submit_status::duplicate;
            }
            return batch_submit_status::buffered;
        }

        submit_ready(std::move(batch), submit_fn);
        drain_ready(submit_fn);
        return batch_submit_status::submitted;
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

template <typename Batch> using BatchSequencer = batch_sequencer<Batch>;

} // namespace af
