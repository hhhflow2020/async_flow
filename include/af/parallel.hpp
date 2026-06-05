#pragma once

#include <cstdint>
#include <vector>

namespace af {

enum class ParallelMode : std::uint8_t {
    NonEmptyOnly,
    AllShards,
    non_empty_only = NonEmptyOnly,
    all_shards = AllShards,
};

enum class OrderedBatchReplayPolicy : std::uint8_t {
    Strict,
    SkipAlreadyApplied,
    strict = Strict,
    skip_already_applied = SkipAlreadyApplied,
};

struct OrderedBatchOptions {
    OrderedBatchReplayPolicy replay_policy{OrderedBatchReplayPolicy::Strict};
};

using parallel_mode = ParallelMode;
using ordered_batch_replay_policy = OrderedBatchReplayPolicy;
using ordered_batch_options = OrderedBatchOptions;

inline constexpr OrderedBatchOptions retryable_ordered_batch_options{
    OrderedBatchReplayPolicy::SkipAlreadyApplied};

template <typename Op> struct ShardedOps {
    std::vector<std::vector<Op>> shards;

    explicit ShardedOps(std::uint16_t shard_count = 0) : shards(shard_count) {}

    [[nodiscard]] std::uint16_t shard_count() const noexcept {
        return static_cast<std::uint16_t>(shards.size());
    }
};

template <typename Op> using sharded_ops = ShardedOps<Op>;

} // namespace af
