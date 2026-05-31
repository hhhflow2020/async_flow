#include "support/runtime_lifecycle_test_support.hpp"

namespace af::test::runtime_lifecycle {

TEST(RuntimeShutdownTests, StartTaskFailsAndDestroysTaskWhenRuntimeIsNotInitialized) {
    NoInitRuntime::shutdown();

    std::atomic<int> destroyed{0};
    EXPECT_FALSE(NoInitRuntime::start_task<NoInitTask>(&destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, MakeTaskHandleDestroysTaskWhenScheduleFailsBeforeInit) {
    NoInitRuntime::shutdown();

    std::atomic<int> destroyed{0};
    {
        auto task = NoInitRuntime::make_task<NoInitTask>();
        EXPECT_FALSE(task->do_it(&destroyed));
        EXPECT_FALSE(task.scheduled());
    }
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyBlocksUntilAcceptedTasksComplete) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownBlockingTask>(
        &started,
        &release,
        &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyAllowsRuntimeThreadRescheduleWhileStopping) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto& value : seen) {
        value.store(WaitShutdownRuntime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownHopDuringStopTask>(
        &started,
        &release,
        &completed,
        &seen));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(seen[0].load(std::memory_order_acquire),
              WaitShutdownRuntime::thread_index(WaitShutdownThread::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire),
              WaitShutdownRuntime::thread_index(WaitShutdownThread::DB_0));
    EXPECT_EQ(WaitShutdownRuntime::unfinished_task_count(), 0U);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyRejectsExternalStartsWhileStopping) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownBlockingTask>(
        &started,
        &release,
        &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool saw_stopping = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (WaitShutdownRuntime::is_stopping()) {
            saw_stopping = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!saw_stopping) {
        release.store(true, std::memory_order_release);
        release.notify_one();
        shutdown_thread.join();
        FAIL() << "runtime did not enter stopping";
    }

    EXPECT_FALSE(WaitShutdownRuntime::start_task<WaitShutdownRejectedTask>(&destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, StopImmediatelyPolicyDoesNotWaitForPendingTasks) {
    FastShutdownRuntime::init();

    std::atomic<int> entered{0};
    ASSERT_TRUE(FastShutdownRuntime::start_task<FastShutdownPendingTask>(&entered));
    ASSERT_TRUE(wait_until_at_least(entered, 1));

    FastShutdownRuntime::shutdown();
    EXPECT_EQ(FastShutdownRuntime::unfinished_task_count(), 0U);
}

TEST(RuntimeShutdownTests, StopImmediatelyTaskRegistryCancelsAndDestroysPendingTasks) {
    FastShutdownRuntime::init();

    std::atomic<int> entered{0};
    std::atomic<int> destroyed{0};
    ASSERT_TRUE(FastShutdownRuntime::start_task<FastShutdownPendingTask>(&entered, &destroyed));
    ASSERT_TRUE(wait_until_at_least(entered, 1));

    FastShutdownRuntime::shutdown();
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}
} // namespace af::test::runtime_lifecycle
