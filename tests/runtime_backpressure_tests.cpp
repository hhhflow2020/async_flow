#include "support/runtime_lifecycle_test_support.hpp"

namespace af::test::runtime_lifecycle {

TEST(RuntimeBackpressureTests, RejectPolicyReturnsFalseAndDeletesRejectedTask) {
    TinyRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(TinyRuntime::start_task<BlockingTinyTask>(&started, &release, &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    EXPECT_TRUE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_TRUE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_FALSE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(TinyRuntime::unfinished_task_count(), 3U);

    release.store(true, std::memory_order_release);
    release.notify_one();
    EXPECT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_TRUE(wait_until_at_least(destroyed, 3));
    EXPECT_EQ(TinyRuntime::unfinished_task_count(), 0U);

    TinyRuntime::shutdown();
}

TEST(RuntimeBackpressureTests, YieldPolicyAllowsManyExternalProducers) {
    YieldRuntime::init();

    constexpr int producer_count = 4;
    constexpr int tasks_per_producer = 200;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([producer, &completed, &all_started] {
            for (int i = 0; i < tasks_per_producer; ++i) {
                const YieldThread target =
                    ((producer + i) & 1) == 0 ? YieldThreads::Logic_0 : YieldThreads::Logic_1;
                if (!YieldRuntime::start_task<YieldCountTask>(target, &completed)) {
                    all_started.store(false, std::memory_order_release);
                    return;
                }
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_TRUE(all_started.load(std::memory_order_acquire));
    EXPECT_TRUE(wait_until_at_least(completed, producer_count * tasks_per_producer));
    YieldRuntime::shutdown();
}

TEST(RuntimeBackpressureTests, YieldPolicyHandlesSameThreadFanoutWithBoundedLocalQueue) {
    YieldRuntime::init();

    constexpr int child_count = 128;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};

    ASSERT_TRUE(YieldRuntime::start_task<YieldFanoutTask>(child_count, &completed, &all_started));
    EXPECT_TRUE(wait_until_at_least(completed, child_count + 1));
    EXPECT_TRUE(all_started.load(std::memory_order_acquire));

    YieldRuntime::shutdown();
}

TEST(RuntimeBackpressureTests, SplitQueuePoliciesRejectFullExternalQueueWithoutBlockingProducer) {
    SplitPolicyRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(
        SplitPolicyRuntime::start_task<SplitPolicyBlockingTask>(&started, &release, &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    EXPECT_TRUE(SplitPolicyRuntime::start_task<SplitPolicyCountTask>(&completed, &destroyed));
    EXPECT_TRUE(SplitPolicyRuntime::start_task<SplitPolicyCountTask>(&completed, &destroyed));
    EXPECT_FALSE(SplitPolicyRuntime::start_task<SplitPolicyCountTask>(&completed, &destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);

    release.store(true, std::memory_order_release);
    release.notify_one();
    EXPECT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_TRUE(wait_until_at_least(destroyed, 3));

    SplitPolicyRuntime::shutdown();
}

TEST(RuntimeBackpressureTests, SplitQueuePoliciesKeepRuntimeThreadFanoutOnYieldPolicy) {
    SplitPolicyRuntime::init();

    constexpr int child_count = 32;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};

    ASSERT_TRUE(SplitPolicyRuntime::start_task<SplitPolicyFanoutTask>(child_count, &completed,
                                                                      &all_started));
    EXPECT_TRUE(wait_until_at_least(completed, child_count + 1));
    EXPECT_TRUE(all_started.load(std::memory_order_acquire));

    SplitPolicyRuntime::shutdown();
}
} // namespace af::test::runtime_lifecycle
