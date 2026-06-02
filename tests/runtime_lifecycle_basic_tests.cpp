#include "support/runtime_lifecycle_test_support.hpp"

namespace af::test::runtime_lifecycle {

TEST_F(RuntimeFixture, OneShotTaskRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThreads::Logic_2, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_2));
}

TEST_F(RuntimeFixture, MakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = Runtime::make_task<ManualStartTask>();
    ASSERT_TRUE(task);
    EXPECT_FALSE(task.scheduled());
    ASSERT_TRUE(task->begin_on(TestThreads::Logic_3, &completed));
    EXPECT_TRUE(task.scheduled());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, UnscheduledCreatedTaskIsDestroyedByHandle) {
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<UnscheduledTask>(&destroyed);
        ASSERT_TRUE(task);
        task->configure_without_schedule();
        EXPECT_FALSE(task.scheduled());
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, CreatedHandleKeepsCompletedTaskAliveUntilReset) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<TrackedDoneTask>(&destroyed);
        ASSERT_TRUE(task->do_it(&completed));
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        Runtime::wait_for_idle();
        EXPECT_EQ(destroyed.load(std::memory_order_acquire), 0);
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, FailedTaskIsReleased) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<FailTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, CancelledTaskIsReleased) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(Runtime::start_task<CancelResultTask>(&completed, &destroyed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(wait_until_at_least(destroyed, 1));
}

TEST_F(RuntimeFixture, StateMachineCanHopThreadsAndAgainOnCurrentThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 4> seen{};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<HopTask>(&completed, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(), Runtime::thread_index(TestThreads::DB_0));
    EXPECT_EQ(seen[2].load(), Runtime::thread_index(TestThreads::Logic_1));
    EXPECT_EQ(seen[3].load(), Runtime::thread_index(TestThreads::Logic_1));
}

TEST_F(RuntimeFixture, WaitForIdleReturnsAfterAcceptedTasksComplete) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThreads::Logic_1, &completed, &ran_on));
    Runtime::wait_for_idle();

    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_1));
    EXPECT_EQ(Runtime::unfinished_task_count(), 0U);
}
} // namespace af::test::runtime_lifecycle
