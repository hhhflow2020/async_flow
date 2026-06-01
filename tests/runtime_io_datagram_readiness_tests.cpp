#include "runtime_io_test_support.hpp"

class IoRuntimeDatagramFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadRejectsDuplicateFdWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> rejected{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<DuplicateWaitRejectedTask>(fds[0], &rejected, &error));
    ASSERT_TRUE(wait_until_at_least(rejected, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EALREADY);

    const char value = 'd';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadResumesTaskWhenFdBecomesWritable) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketWritableTask>(fds[0], &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadReportsPeerHangupAsClosedRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> closed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketHangupTask>(fds[0], &armed, &closed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ::close(fds[1]);
    fds[1] = -1;
    ASSERT_TRUE(wait_until_at_least(closed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
