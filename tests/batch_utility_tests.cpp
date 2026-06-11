#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "af/batch_sequencer.hpp"
#include "af/crud_batch.hpp"
#include "af/runtime.hpp"

TEST(UtilityTests, SplitByShardGroupsByKey) {
    struct Op {
        std::uint64_t key;
        int value;
    };

    std::vector<Op> ops{{0, 10}, {1, 11}, {4, 14}, {6, 16}};
    auto sharded =
        af::runtime::split_by_shard(std::move(ops), 4, [](const Op &op) { return op.key; });

    ASSERT_EQ(sharded.shard_count(), 4);
    ASSERT_EQ(sharded.shards[0].size(), 2);
    ASSERT_EQ(sharded.shards[1].size(), 1);
    ASSERT_EQ(sharded.shards[2].size(), 1);
    ASSERT_TRUE(sharded.shards[3].empty());
}

TEST(UtilityTests, SplitCrudOpsGroupsByKeyAndKeepsOperationData) {
    std::vector<af::crud_op<std::uint64_t, int>> ops{
        {af::op_type::add, 0, 10},
        {af::op_type::update, 5, 15},
        {af::op_type::delete_op, 2, 20},
    };

    auto sharded = af::split_crud_ops(std::move(ops), 4);

    ASSERT_EQ(sharded.shard_count(), 4);
    ASSERT_EQ(sharded.shards[0].size(), 1);
    ASSERT_EQ(sharded.shards[1].size(), 1);
    ASSERT_EQ(sharded.shards[2].size(), 1);
    EXPECT_EQ(sharded.shards[0][0].type, af::op_type::add);
    EXPECT_EQ(sharded.shards[0][0].value, 10);
    EXPECT_EQ(sharded.shards[1][0].type, af::op_type::update);
    EXPECT_EQ(sharded.shards[1][0].value, 15);
    EXPECT_EQ(sharded.shards[2][0].type, af::op_type::delete_op);
    EXPECT_EQ(sharded.shards[2][0].value, 20);
}

TEST(UtilityTests, SplitChangeBatchSupportsCustomShardFunction) {
    af::change_batch<std::uint64_t, int> batch{
        7,
        {
            {af::op_type::add, 10, 1},
            {af::op_type::update, 11, 2},
            {af::op_type::delete_op, 12, 3},
        },
    };

    auto sharded = af::split_change_batch(batch, 2, [](std::uint64_t key) { return key / 10U; });

    EXPECT_EQ(batch.batch_id, 7U);
    EXPECT_TRUE(batch.ops.empty());
    ASSERT_EQ(sharded.shard_count(), 2);
    EXPECT_TRUE(sharded.shards[0].empty());
    ASSERT_EQ(sharded.shards[1].size(), 3);
    EXPECT_EQ(sharded.shards[1][0].key, 10U);
    EXPECT_EQ(sharded.shards[1][1].key, 11U);
    EXPECT_EQ(sharded.shards[1][2].key, 12U);
}

TEST(UtilityTests, BatchSequencerBuffersOutOfOrderBatches) {
    af::batch_sequencer<int> sequencer(1);
    std::vector<int> submitted;

    auto submit = [&](int value) { submitted.push_back(value); };

    EXPECT_EQ(sequencer.submit(2, 20, submit), af::batch_submit_status::buffered);
    EXPECT_EQ(sequencer.submit(2, 200, submit), af::batch_submit_status::duplicate);
    EXPECT_TRUE(submitted.empty());
    EXPECT_EQ(sequencer.submit(1, 10, submit), af::batch_submit_status::submitted);
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_EQ(submitted[0], 10);
    EXPECT_EQ(submitted[1], 20);
    EXPECT_EQ(sequencer.submit(1, 10, submit), af::batch_submit_status::duplicate);
}

TEST(UtilityTests, OrderedBatchRetrySkipPolicyTracksRetryAndSkipDecisions) {
    af::ordered_batch_retry_skip_policy<std::uint64_t> policy({
        .max_retries = 2,
        .skip_after_retries = true,
    });

    auto first = policy.record_failure(7U);
    EXPECT_TRUE(first.should_retry());
    EXPECT_EQ(first.failure_count, 1U);

    auto second = policy.record_failure(7U);
    EXPECT_TRUE(second.should_retry());
    EXPECT_EQ(second.failure_count, 2U);

    auto third = policy.record_failure(7U);
    EXPECT_TRUE(third.should_skip());
    EXPECT_EQ(third.failure_count, 3U);
    EXPECT_EQ(policy.failure_count(7U), 3U);

    policy.record_success(7U);
    EXPECT_EQ(policy.failure_count(7U), 0U);
    EXPECT_TRUE(policy.record_failure(7U).should_retry());
}

TEST(UtilityTests, OrderedBatchRetrySkipPolicyCanStopInsteadOfSkipping) {
    af::ordered_batch_retry_skip_policy<std::uint64_t> policy({
        .max_retries = 0,
        .skip_after_retries = false,
    });

    auto decision = policy.record_failure(9U);
    EXPECT_TRUE(decision.should_stop());
    EXPECT_EQ(decision.failure_count, 1U);
}
