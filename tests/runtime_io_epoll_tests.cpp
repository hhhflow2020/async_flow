#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeEpollFixture, IoThreadUsesConfiguredThreadKindAndAcceptsTasks) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};

    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
}

TEST_F(IoRuntimeEpollFixture, WorkerThreadDoesNotExposeIoBackend) {
    EXPECT_FALSE(IoRuntime::io_backend_available(IoTestThread::Logic_0));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<WorkerIoWaitRejectedTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_NE(error.load(std::memory_order_acquire), 0);
}
