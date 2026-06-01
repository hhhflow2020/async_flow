#include "support/runtime_parallel_test_support.hpp"

namespace af::test::runtime_parallel {

TEST_F(ParallelRuntimeFixture, OrderedStartTaskBuffersOutOfOrderBatches) {
    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{2, 20, &applied, &completed})));
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{3, 30, &applied, &completed})));

    ASSERT_TRUE(wait_until_at_least(completed, 3));
    ASSERT_EQ(applied.size(), 3);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
    EXPECT_EQ(applied[2], 30);
}

TEST_F(ParallelRuntimeFixture, OrderedStartTaskKeepsGapBufferedUntilMissingBatchArrives) {
    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{2, 20, &applied, &completed})));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);
    EXPECT_TRUE(applied.empty());

    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE(wait_until_at_least(completed, 2));
    ASSERT_EQ(applied.size(), 2);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST_F(ParallelRuntimeFixture, OrderedStartTaskIgnoresDuplicateAndOldBatches) {
    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 100, &applied, &completed})));
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{2, 20, &applied, &completed})));
    ASSERT_TRUE(wait_until_at_least(completed, 2));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 2);
    ASSERT_EQ(applied.size(), 2);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST_F(ParallelRuntimeFixture, OrderedStartDoesNotAdvanceWhenApplyStartFails) {
    std::atomic<int> attempts{0};
    std::atomic<int> completed{0};
    std::atomic<bool> fail_first_start{false};
    std::vector<int> applied;

    ASSERT_TRUE(
        (Runtime::start_ordered_task<OrderedStartFailureStream, OrderedStartFailingApplyTask>(
            TestThread::DB_0,
            OrderedStartFailureBatch{1, 10, &applied, &attempts, &completed, &fail_first_start})));
    ASSERT_TRUE(wait_until_at_least(attempts, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(
        (Runtime::start_ordered_task<OrderedStartFailureStream, OrderedStartFailingApplyTask>(
            TestThread::DB_0,
            OrderedStartFailureBatch{2, 20, &applied, &attempts, &completed, &fail_first_start})));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(attempts.load(std::memory_order_acquire), 1);
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(
        (Runtime::start_ordered_task<OrderedStartFailureStream, OrderedStartFailingApplyTask>(
            TestThread::DB_0,
            OrderedStartFailureBatch{1, 10, &applied, &attempts, &completed, &fail_first_start})));
    ASSERT_TRUE(wait_until_at_least(completed, 2));

    ASSERT_EQ(applied.size(), 2);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST(RuntimeOrderedStartTests, OrderedStartStateResetsAfterRuntimeRestart) {
    Runtime::init();

    std::atomic<int> first_completed{0};
    std::vector<int> first_applied;
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 10, &first_applied, &first_completed})));
    ASSERT_TRUE(wait_until_at_least(first_completed, 1));
    Runtime::shutdown();

    Runtime::init();
    std::atomic<int> second_completed{0};
    std::vector<int> second_applied;
    ASSERT_TRUE((Runtime::start_ordered_task<OrderedStartStream, OrderedStartApplyTask>(
        TestThread::DB_0, OrderedStartBatch{1, 20, &second_applied, &second_completed})));
    ASSERT_TRUE(wait_until_at_least(second_completed, 1));
    Runtime::shutdown();

    ASSERT_EQ(first_applied.size(), 1);
    ASSERT_EQ(second_applied.size(), 1);
    EXPECT_EQ(first_applied[0], 10);
    EXPECT_EQ(second_applied[0], 20);
}
} // namespace af::test::runtime_parallel
