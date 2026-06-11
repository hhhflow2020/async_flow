#pragma once

#include <cstdint>
#include <vector>

namespace af {

enum class parallel_mode : std::uint8_t {
    non_empty_only,
    all_shards,
};

enum class ordered_batch_replay_policy : std::uint8_t {
    strict,
    skip_already_applied,
};

struct ordered_batch_options {
    ordered_batch_replay_policy replay_policy{ordered_batch_replay_policy::strict};
};

inline constexpr ordered_batch_options retryable_ordered_batch_options{
    ordered_batch_replay_policy::skip_already_applied};

template <typename Op> struct sharded_ops {
    std::vector<std::vector<Op>> shards;

    explicit sharded_ops(std::uint16_t shard_count = 0) : shards(shard_count) {}

    [[nodiscard]] std::uint16_t shard_count() const noexcept {
        return static_cast<std::uint16_t>(shards.size());
    }
};

} // namespace af
