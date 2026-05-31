#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeEpollFixture, SocketLifecycleHelpersRunOnIoThread) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<int> reuse_value{0};
    std::atomic<int> local_port{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};
    ASSERT_TRUE(IoRuntime::start_task<SocketLifecycleSetupTask>(
        &completed,
        &error,
        &reuse_value,
        &local_port,
        &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_NE(reuse_value.load(std::memory_order_acquire), 0);
    EXPECT_GT(local_port.load(std::memory_order_acquire), 0);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
#else
    GTEST_SKIP() << "socket lifecycle helpers are Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, SocketLifecycleHelpersHandleInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    ASSERT_TRUE(IoRuntime::start_task<SocketLifecycleBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
#else
    GTEST_SKIP() << "socket lifecycle helpers are Linux-only";
#endif
}
