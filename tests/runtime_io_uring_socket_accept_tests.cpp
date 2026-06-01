#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketAcceptFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketAcceptFixture, IoUringThreadAcceptsTcpConnectionOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptTask>(listener, &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr *>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketAcceptFixture, IoUringAcceptDirectReceivesThroughFixedFile) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<int> packed_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptDirectTask>(listener, &armed, &completed,
                                                                     &error, &packed_read));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int submit_error = error.load(std::memory_order_acquire);
        if (submit_error == EINVAL || submit_error == EBADF || submit_error == ENOSYS ||
            submit_error == ENXIO
#ifdef EOPNOTSUPP
            || submit_error == EOPNOTSUPP
#endif
        ) {
            close_fd(listener);
            GTEST_SKIP() << "io_uring direct accept unsupported";
        }
        close_fd(listener);
        FAIL() << "direct accept was not armed, error=" << submit_error;
    }

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr *>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);
    const char request[2]{'A', 'B'};
    ASSERT_TRUE(write_exact_until(client, request, sizeof(request)));

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL || task_error == EBADF || task_error == ENOSYS || task_error == ENXIO
#ifdef EOPNOTSUPP
        || task_error == EOPNOTSUPP
#endif
    ) {
        close_fd(client);
        close_fd(listener);
        GTEST_SKIP() << "io_uring direct accept unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(packed_read.load(std::memory_order_acquire), ('A' << 8) | 'B');

    char response[2]{};
    ASSERT_TRUE(read_exact_until(client, response, sizeof(response)));
    EXPECT_EQ(response[0], 'O');
    EXPECT_EQ(response[1], 'K');

    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketAcceptFixture, IoUringAcceptMultishotAcceptsMultipleConnections) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    constexpr int target_accepts = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> accepted_count{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptMultishotTask>(
        listener, target_accepts, &armed, &completed, &accepted_count, &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int submit_error = error.load(std::memory_order_acquire);
        if (submit_error == EINVAL || submit_error == EOPNOTSUPP || submit_error == ENOSYS) {
            close_fd(listener);
            GTEST_SKIP() << "io_uring multishot accept unsupported";
        }
        FAIL() << "multishot accept was not armed, error=" << submit_error;
    }

    int clients[target_accepts]{-1, -1};
    for (int &client : clients) {
        client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        ASSERT_GE(client, 0);
        const int rc = ::connect(client, reinterpret_cast<sockaddr *>(&address), address_size);
        ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);
    }

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(accepted_count.load(std::memory_order_acquire), target_accepts);

    for (int client : clients) {
        close_fd(client);
    }
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketAcceptFixture, IoUringAcceptMultishotCancelDrainsQueuedAccepts) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    constexpr int target_accepts = 1;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> accepted_count{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptMultishotTask>(
        listener, target_accepts, &armed, &completed, &accepted_count, &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int submit_error = error.load(std::memory_order_acquire);
        if (submit_error == EINVAL || submit_error == EOPNOTSUPP || submit_error == ENOSYS) {
            close_fd(listener);
            GTEST_SKIP() << "io_uring multishot accept unsupported";
        }
        FAIL() << "multishot accept was not armed, error=" << submit_error;
    }

    int clients[4]{-1, -1, -1, -1};
    for (int &client : clients) {
        client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        ASSERT_GE(client, 0);
        const int rc = ::connect(client, reinterpret_cast<sockaddr *>(&address), address_size);
        ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);
    }

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(accepted_count.load(std::memory_order_acquire), target_accepts);

    for (int client : clients) {
        close_fd(client);
    }
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
