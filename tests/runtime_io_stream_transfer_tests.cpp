#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};
class UringIoRuntimePollFixture : public UringIoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendfileSendsFileToSocket) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-sendfile-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload[] = "asyncflow-sendfile";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    ASSERT_EQ(::write(file, payload, payload_size), static_cast<ssize_t>(payload_size));
    ASSERT_EQ(::lseek(file, 0, SEEK_SET), 0);

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendfileSocketTask>(
        fds[0],
        file,
        payload_size,
        3,
        true,
        &completed,
        &calls,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);
    EXPECT_GT(calls.load(std::memory_order_acquire), 1);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(fds[1], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, SendfileWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-sendfile-pending-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'P';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    int server = accept_tcp_until_ready(listener);
    ASSERT_GE(server, 0);
    int send_buffer = 4096;
    ASSERT_EQ(
        ::setsockopt(
            server,
            SOL_SOCKET,
            SO_SNDBUF,
            &send_buffer,
            static_cast<socklen_t>(sizeof(send_buffer))),
        0);
    ASSERT_TRUE(fill_until_blocked(server));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendfileTask>(
        server,
        file,
        &pending_seen,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(client);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_fd(server);
    close_fd(client);
    close_fd(listener);
    close_fd(file);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(UringIoRuntimePollFixture, IoUringPollReadinessResumesSendfileWhenSocketWritable) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-poll-sendfile-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'R';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        fds[0],
        file,
        &state,
        &wait_kind,
        &pending_seen,
        &completed,
        &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    EXPECT_EQ(
        wait_kind.load(std::memory_order_acquire),
        static_cast<int>(af::IoWaitKind::Readiness));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimePollFixture, IoUringPollReadinessCancelPendingSendfileWait) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-poll-cancel-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'C';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        fds[0],
        file,
        &state,
        &wait_kind,
        &pending_seen,
        &completed,
        &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    ASSERT_EQ(
        wait_kind.load(std::memory_order_acquire),
        static_cast<int>(af::IoWaitKind::Readiness));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(
        &state,
        &cancel_completed,
        &cancel_result,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 0U);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, SpliceTransfersPipeContentWithNullOffsets) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int input[2]{-1, -1};
    int output[2]{-1, -1};
    ASSERT_EQ(::pipe2(input, O_NONBLOCK | O_CLOEXEC), 0);
    ASSERT_EQ(::pipe2(output, O_NONBLOCK | O_CLOEXEC), 0);

    const char payload[] = "splice-through-kernel";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    ASSERT_EQ(::write(input[1], payload, payload_size), static_cast<ssize_t>(payload_size));

    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_spliced{0};
    ASSERT_TRUE(IoRuntime::start_task<SplicePipeTask>(
        input[0],
        output[1],
        payload_size,
        &completed,
        &bytes_spliced));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_spliced.load(std::memory_order_acquire), payload_size);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(output[0], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(input);
    close_pair(output);
#else
    GTEST_SKIP() << "splice is Linux-only";
#endif
}
