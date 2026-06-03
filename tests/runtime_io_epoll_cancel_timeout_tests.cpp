#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsPendingReadWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};

    ASSERT_TRUE(IoRuntime::start_task<CancellableSocketReadTask>(fds[0], &state, &armed,
                                                                 &read_completed, &read_error));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state, true, &cancel_completed, &first_cancel, &second_cancel, &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(second_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadRejectsCancelForIdleState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> result{-1};
    std::atomic<int> error{0};
    std::atomic<int> token_cleared{0};
    ASSERT_TRUE(
        IoRuntime::start_task<CancelIdleIoStateTask>(&completed, &result, &error, &token_cleared));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(result.load(std::memory_order_acquire), 0);
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOENT);
    EXPECT_EQ(token_cleared.load(std::memory_order_acquire), 1);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadTimesOutPendingRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0], std::chrono::milliseconds(1), &state, &armed, &completed, &error, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ETIMEDOUT);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsTimeoutWhenReadCompletes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0], std::chrono::seconds(1), &state, &armed, &completed, &error, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 't';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsTimeoutWhenIoIsCanceled) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(fds[0], std::chrono::milliseconds(20),
                                                             &state, &armed, &read_completed,
                                                             &read_error, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state, false, &cancel_completed, &first_cancel, &second_cancel, &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
