#include "support/runtime_parallel_test_support.hpp"

namespace af::test::runtime_parallel {

TEST_F(ParallelRuntimeFixture, OrderedBatchRunsEveryShardAndAcceptsContiguousBatches) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::array<std::atomic<std::uint64_t>, 4> batch_seen{};

    ASSERT_TRUE(Runtime::start_task<OrderedTask>(1U, &completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 1);
        EXPECT_EQ(batch_seen[i].load(), 1U);
    }

    ASSERT_TRUE(Runtime::start_task<OrderedTask>(2U, &completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_until_at_least(completed, 2));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 2);
        EXPECT_EQ(batch_seen[i].load(), 2U);
    }
}

TEST_F(ParallelRuntimeFixture, OrderedBatchConvenienceOverloadRunsAllShards) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};

    ASSERT_TRUE(Runtime::start_task<OrderedOverloadTask>(1U, &completed, &shard_hits));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(std::memory_order_acquire), 1);
        EXPECT_EQ(
            Runtime::ordered_last_applied_batch_id(Runtime::thread_from_index(i)),
            1U);
    }
}

TEST_F(ParallelRuntimeFixture, OrderedBatchFailureDoesNotAdvanceFailedShard) {
    std::atomic<int> completed{0};
    std::atomic<std::uint32_t> failures{0};

    ASSERT_TRUE(Runtime::start_task<OrderedFailureTask>(1U, 1U, &completed, &failures));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(failures.load(std::memory_order_acquire), 1U);
    EXPECT_EQ(Runtime::ordered_last_applied_batch_id(TestThread::Logic_0), 1U);
    EXPECT_EQ(Runtime::ordered_last_applied_batch_id(TestThread::Logic_1), 0U);
    EXPECT_EQ(Runtime::ordered_last_applied_batch_id(TestThread::Logic_2), 1U);
    EXPECT_EQ(Runtime::ordered_last_applied_batch_id(TestThread::Logic_3), 1U);
}

TEST_F(ParallelRuntimeFixture, RetryableOrderedBatchSkipsAlreadyAppliedShards) {
    std::atomic<int> failed_completed{0};
    std::atomic<std::uint32_t> failed_failures{0};

    ASSERT_TRUE(Runtime::start_task<OrderedFailureTask>(
        1U,
        1U,
        &failed_completed,
        &failed_failures));
    ASSERT_TRUE(wait_until_at_least(failed_completed, 1));
    ASSERT_EQ(failed_failures.load(std::memory_order_acquire), 1U);

    std::atomic<int> retry_completed{0};
    std::atomic<std::uint32_t> retry_failures{0};
    std::array<std::atomic<int>, 4> shard_hits{};

    ASSERT_TRUE(Runtime::start_task<OrderedRetryableTask>(
        1U,
        &retry_completed,
        &shard_hits,
        &retry_failures));
    ASSERT_TRUE(wait_until_at_least(retry_completed, 1));

    EXPECT_EQ(retry_failures.load(std::memory_order_acquire), 0U);
    EXPECT_EQ(shard_hits[0].load(std::memory_order_acquire), 0);
    EXPECT_EQ(shard_hits[1].load(std::memory_order_acquire), 1);
    EXPECT_EQ(shard_hits[2].load(std::memory_order_acquire), 0);
    EXPECT_EQ(shard_hits[3].load(std::memory_order_acquire), 0);
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(
            Runtime::ordered_last_applied_batch_id(Runtime::thread_from_index(i)),
            1U);
    }
}
} // namespace af::test::runtime_parallel
