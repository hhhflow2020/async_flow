#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};
class UringIoRuntimePollFixture : public UringIoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, StreamAdapterReceivesAndSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<StreamAdapterEchoTask>(
        fds[0],
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request = 'Q';
    ASSERT_EQ(::write(fds[1], &request, sizeof(request)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), request);

    char response = 0;
    ASSERT_EQ(::read(fds[1], &response, sizeof(response)), 1);
    EXPECT_EQ(response, 'R');

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, StreamAdapterShutdownWriteHalfClosesPeer) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<StreamShutdownTask>(fds[0], &completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);

    char ignored = 0;
    EXPECT_EQ(::read(fds[1], &ignored, sizeof(ignored)), 0);
    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, StreamAdapterReceivesAndSendsVectoredSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> request_seen{0};
    ASSERT_TRUE(IoRuntime::start_task<StreamVectoredEchoTask>(
        fds[0],
        &armed,
        &completed,
        &request_seen));
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

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendZcSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    const char payload[] = "asyncflow-send-zc";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendZcSocketTask>(
        fds[0],
        payload,
        payload_size,
        3,
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
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendvZcSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
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
    ASSERT_TRUE(IoRuntime::start_task<SendvZcSocketTask>(
        fds[0],
        first,
        first_size,
        second,
        second_size,
        &completed,
        &bytes_sent));
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

TEST_F(IoRuntimeStreamFixture, SendZcWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendZcTask>(
        fds[0],
        &pending_seen,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}

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

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendfileTask>(
        fds[0],
        file,
        &pending_seen,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
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

TEST_F(IoRuntimeStreamFixture, EpollIoThreadAcceptsTcpConnectionFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
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
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
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
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
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
        listener,
        &completed,
        &invalid_error,
        &null_error,
        &address_error,
        &unavailable_error));
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
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
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
        fds[0],
        &completed,
        &invalid_error,
        &null_error,
        &unavailable_error,
        &register_error));
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
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
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
