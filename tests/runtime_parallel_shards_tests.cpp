#include "support/runtime_parallel_test_support.hpp"

namespace af::test::runtime_parallel {

TEST_F(ParallelRuntimeFixture, ParallelShardsNonEmptyOnlySkipsEmptyShards) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    ASSERT_TRUE(Runtime::start_task<ParallelTask>(
        af::ParallelMode::NonEmptyOnly,
        &completed,
        &shard_hits,
        &sum));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(shard_hits[0].load(), 1);
    EXPECT_EQ(shard_hits[1].load(), 0);
    EXPECT_EQ(shard_hits[2].load(), 1);
    EXPECT_EQ(shard_hits[3].load(), 0);
    EXPECT_EQ(sum.load(), 6);
}

TEST_F(ParallelRuntimeFixture, ParallelShardsAllShardsRunsNoopShards) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    ASSERT_TRUE(Runtime::start_task<ParallelTask>(
        af::ParallelMode::AllShards,
        &completed,
        &shard_hits,
        &sum));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    for (const auto& hit : shard_hits) {
        EXPECT_EQ(hit.load(), 1);
    }
    EXPECT_EQ(sum.load(), 6);
}

TEST_F(ParallelRuntimeFixture, ParallelShardFailuresAreVisibleToOwner) {
    std::atomic<int> completed{0};
    std::atomic<std::uint32_t> failures{0};

    ASSERT_TRUE(Runtime::start_task<ParallelFailureTask>(&completed, &failures));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(failures.load(std::memory_order_acquire), 1U);
}

TEST_F(ParallelRuntimeFixture, EmptyNonEmptyParallelResumesOwner) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<EmptyParallelTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(ParallelRuntimeFixture, ParallelShardsDefaultBeginOverloadUsesThreadZero) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<DefaultParallelOverloadTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThread::Logic_1));
}
} // namespace af::test::runtime_parallel
