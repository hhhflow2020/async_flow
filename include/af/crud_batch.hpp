#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/parallel.hpp"

namespace af {

enum class OpType : std::uint8_t {
    Add,
    Update,
    Delete,
};

template <typename Key, typename Value> struct CrudOp {
    OpType type{OpType::Add};
    Key key{};
    Value value{};
};

template <typename Key, typename Value> struct ChangeBatch {
    std::uint64_t batch_id{0};
    std::vector<CrudOp<Key, Value>> ops;
};

template <typename Key, typename Value, typename ShardFn>
[[nodiscard]] ShardedOps<CrudOp<Key, Value>> split_crud_ops(std::vector<CrudOp<Key, Value>> &&ops,
                                                            std::uint16_t shard_count,
                                                            ShardFn &&shard_fn) {
    AF_ASSERT(shard_count > 0);
    ShardedOps<CrudOp<Key, Value>> sharded(shard_count);
    if (shard_count == 0) {
        return sharded;
    }

    for (auto &op : ops) {
        const auto shard_value = static_cast<std::uint64_t>(shard_fn(op.key));
        const auto shard = static_cast<std::uint16_t>(shard_value % shard_count);
        sharded.shards[shard].push_back(std::move(op));
    }
    return sharded;
}

template <typename Key, typename Value>
[[nodiscard]] ShardedOps<CrudOp<Key, Value>> split_crud_ops(std::vector<CrudOp<Key, Value>> &&ops,
                                                            std::uint16_t shard_count) {
    return split_crud_ops(std::move(ops), shard_count,
                          [](const Key &key) noexcept { return static_cast<std::uint64_t>(key); });
}

template <typename Key, typename Value, typename ShardFn>
[[nodiscard]] ShardedOps<CrudOp<Key, Value>>
split_change_batch(ChangeBatch<Key, Value> &batch, std::uint16_t shard_count, ShardFn &&shard_fn) {
    auto ops = std::move(batch.ops);
    batch.ops.clear();
    return split_crud_ops(std::move(ops), shard_count, std::forward<ShardFn>(shard_fn));
}

template <typename Key, typename Value>
[[nodiscard]] ShardedOps<CrudOp<Key, Value>> split_change_batch(ChangeBatch<Key, Value> &batch,
                                                                std::uint16_t shard_count) {
    auto ops = std::move(batch.ops);
    batch.ops.clear();
    return split_crud_ops(std::move(ops), shard_count);
}

} // namespace af
