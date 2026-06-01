#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeEpollFixture, EpollIoThreadResumesTaskWhenFdBecomesReadable) {
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

    const char value = 'x';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadRearmsReadableFdWithSameState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> reads{0};
    char output[2]{};

    ASSERT_TRUE(IoRuntime::start_task<SocketRepeatedReadableTask>(fds[0], &armed, &completed,
                                                                  &reads, output));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char first = 'a';
    ASSERT_EQ(::write(fds[1], &first, sizeof(first)), 1);
    ASSERT_TRUE(wait_until_at_least(reads, 1));
    ASSERT_TRUE(wait_until_at_least(armed, 2));

    const char second = 'b';
    ASSERT_EQ(::write(fds[1], &second, sizeof(second)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(output[0], first);
    EXPECT_EQ(output[1], second);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
