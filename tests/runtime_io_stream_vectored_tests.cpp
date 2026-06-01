#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, StreamAdapterReceivesAndSendsVectoredSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> request_seen{0};
    ASSERT_TRUE(
        IoRuntime::start_task<StreamVectoredEchoTask>(fds[0], &armed, &completed, &request_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request[2]{'A', 'B'};
    ASSERT_EQ(::write(fds[1], request, sizeof(request)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(request_seen.load(std::memory_order_acquire), ('A' << 8) | 'B');

    char response[2]{};
    ASSERT_EQ(::read(fds[1], response, sizeof(response)), 2);
    EXPECT_EQ(response[0], 'X');
    EXPECT_EQ(response[1], 'Y');

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendvZcSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    const char first[] = "asyncflow-";
    const char second[] = "sendmsg-zc";
    constexpr std::size_t first_size = sizeof(first) - 1U;
    constexpr std::size_t second_size = sizeof(second) - 1U;
    constexpr std::size_t payload_size = first_size + second_size;
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendvZcSocketTask>(fds[0], first, first_size, second,
                                                         second_size, &completed, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(fds[1], received, payload_size));
    EXPECT_EQ(std::memcmp(received, first, first_size), 0);
    EXPECT_EQ(std::memcmp(received + first_size, second, second_size), 0);

    close_pair(fds);
#else
    GTEST_SKIP() << "sendmsg_zc helper is Linux-only";
#endif
}
