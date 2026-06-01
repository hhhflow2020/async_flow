#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendZcSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    const char payload[] = "asyncflow-send-zc";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendZcSocketTask>(fds[0], payload, payload_size, 3,
                                                        &completed, &calls, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);
    EXPECT_GT(calls.load(std::memory_order_acquire), 1);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(fds[1], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(fds);
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, SendZcWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(
        IoRuntime::start_task<PendingSendZcTask>(fds[0], &pending_seen, &completed, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}
