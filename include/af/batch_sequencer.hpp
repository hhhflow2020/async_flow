#pragma once

#include <cstdint>
#include <map>
#include <utility>

namespace af {

enum class BatchSubmitStatus {
    Submitted,
    Buffered,
    Duplicate,
};

template <typename Batch>
class BatchSequencer {
public:
    explicit BatchSequencer(std::uint64_t first_batch_id = 1)
        : next_batch_id_(first_batch_id) {}

    [[nodiscard]] std::uint64_t next_batch_id() const noexcept {
        return next_batch_id_;
    }

    template <typename SubmitFn>
    BatchSubmitStatus submit(std::uint64_t batch_id, Batch batch, SubmitFn&& submit_fn) {
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
    template <typename SubmitFn>
    void submit_ready(Batch batch, SubmitFn& submit_fn) {
        submit_fn(std::move(batch));
        ++next_batch_id_;
    }

    template <typename SubmitFn>
    void drain_ready(SubmitFn& submit_fn) {
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
    std::map<std::uint64_t, Batch> pending_;
};

} // namespace af
