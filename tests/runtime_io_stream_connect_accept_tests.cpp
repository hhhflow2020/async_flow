#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, EpollIoThreadAcceptsTcpConnectionFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpAcceptTask>(listener, &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr *>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, AcceptMultishotReportsInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> completed{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    std::atomic<int> address_error{0};
    std::atomic<int> unavailable_error{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpAcceptMultishotBoundaryTask>(
        listener, &completed, &invalid_error, &null_error, &address_error, &unavailable_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(address_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);

    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, RecvMultishotReportsInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    std::atomic<int> unavailable_error{0};
    std::atomic<int> register_error{0};
    ASSERT_TRUE(IoRuntime::start_task<RecvMultishotBoundaryTask>(
        fds[0], &completed, &invalid_error, &null_error, &unavailable_error, &register_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(register_error.load(std::memory_order_acquire), ENOSYS);

    close_pair(fds);
#else
    GTEST_SKIP() << "provided buffer recv_multishot is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, EpollIoThreadConnectsTcpStreamFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpConnectTask>(client, address, address_size, &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    int accepted = accept_tcp_until_ready(listener);
    ASSERT_GE(accepted, 0);
    close_fd(accepted);
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
