#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketStreamFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadSendsStreamBytesOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamSendTask>(fds[0], &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    char value = 0;
    ASSERT_EQ(::read(fds[1], &value, sizeof(value)), 1);
    EXPECT_EQ(value, 'S');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadCreatesSocketOrFallsBack) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    ASSERT_TRUE(UringIoRuntime::start_task<UringSocketCreateTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadSendZcSendsStreamBytesOrFallsBack) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamSendZcTask>(fds[0], &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    char value = 0;
    ASSERT_EQ(::read(fds[1], &value, sizeof(value)), 1);
    EXPECT_EQ(value, 'Z');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadShutdownsStreamOrFallsBack) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamShutdownTask>(fds[0], &completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);

    char ignored = 0;
    EXPECT_EQ(::read(fds[1], &ignored, sizeof(ignored)), 0);
    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadHandlesVectoredStreamOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> request_seen{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamVectoredTask>(
        fds[0],
        &armed,
        &completed,
        &request_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request[2]{'C', 'D'};
    ASSERT_EQ(::write(fds[1], request, sizeof(request)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(request_seen.load(std::memory_order_acquire), ('C' << 8) | 'D');

    char response[2]{};
    ASSERT_EQ(::read(fds[1], response, sizeof(response)), 2);
    EXPECT_EQ(response[0], 'U');
    EXPECT_EQ(response[1], 'V');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringCompletionCancelIsNotConsumableBeforeCqe) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> wait_kind{-1};
    std::atomic<int> cancel_result{0};
    std::atomic<int> immediate_pending{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringSelfCancelRecvCompletionTask>(
        fds[0],
        &wait_kind,
        &cancel_result,
        &immediate_pending,
        &completed,
        &error));

    if (!wait_until_at_least(completed, 1)) {
        const char value = 'x';
        static_cast<void>(::write(fds[1], &value, sizeof(value)));
        ASSERT_TRUE(wait_until_at_least(completed, 1));
    }

    if (wait_kind.load(std::memory_order_acquire) !=
        static_cast<int>(af::IoWaitKind::Completion)) {
        close_pair(fds);
        GTEST_SKIP() << "recv did not remain as an io_uring completion operation";
    }

    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(immediate_pending.load(std::memory_order_acquire), 1);
    EXPECT_EQ(error.load(std::memory_order_acquire), ECANCELED);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketStreamFixture, IoUringThreadConnectsTcpStreamOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpConnectTask>(
        client,
        address,
        address_size,
        &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    int accepted = accept_tcp_until_ready(listener);
    ASSERT_GE(accepted, 0);
    close_fd(accepted);
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
