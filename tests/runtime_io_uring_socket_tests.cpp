#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadFallsBackToEpollReadiness) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamFallbackTask>(
        fds[0],
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'U';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringTimeoutCompletesInRing) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTimeoutTask>(&armed, &completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    if (error.load(std::memory_order_acquire) == ENOSYS ||
        error.load(std::memory_order_acquire) == EINVAL) {
        GTEST_SKIP() << "io_uring timeout operation unsupported";
    }
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_GE(armed.load(std::memory_order_acquire), 1);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadCancelsPendingRecvCompletion) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> armed{0};
    std::atomic<int> recv_completed{0};
    std::atomic<int> recv_error{0};
    std::atomic<int> recv_bytes{-1};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancellableSocketRecvTask>(
        fds[0],
        &state,
        &wait_kind,
        &armed,
        &recv_completed,
        &recv_error,
        &recv_bytes));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    if (wait_kind.load(std::memory_order_acquire) !=
        static_cast<int>(af::IoWaitKind::Completion)) {
        const char value = 'f';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
        close_pair(fds);
        GTEST_SKIP() << "recv did not remain as an io_uring completion operation";
    }

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(
        &state,
        &cancel_completed,
        &cancel_result,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));

    if (!wait_until_at_least(recv_completed, 1)) {
        const char value = 'u';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
    }

    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_bytes.load(std::memory_order_acquire), -1);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadHandlesTimerFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd timer = af::make_timerfd();
    ASSERT_TRUE(timer);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> expirations{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTimerFdTask>(
        timer.get(),
        &armed,
        &completed,
        &expirations));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::arm_timerfd_after(timer.get(), std::chrono::milliseconds(1), error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_GE(expirations.load(std::memory_order_acquire), std::uint64_t{1});
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadHandlesEventFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> value{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringEventFdTask>(
        event.get(),
        &armed,
        &completed,
        &value));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::write_eventfd(event.get(), 9, error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(value.load(std::memory_order_acquire), std::uint64_t{9});
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadSendsStreamBytesOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadCreatesSocketOrFallsBack) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadSendZcSendsStreamBytesOrFallsBack) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadShutdownsStreamOrFallsBack) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadHandlesVectoredStreamOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadAcceptsTcpConnectionOrFallsBackToEpoll) {
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
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringAcceptDirectReceivesThroughFixedFile) {
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
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptDirectTask>(
        listener,
        &armed,
        &completed,
        &error,
        &packed_read));

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
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);
    const char request[2]{'A', 'B'};
    ASSERT_TRUE(write_exact_until(client, request, sizeof(request)));

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL || task_error == EBADF || task_error == ENOSYS ||
        task_error == ENXIO
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

TEST_F(UringIoRuntimeSocketFixture, IoUringAcceptMultishotAcceptsMultipleConnections) {
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
        listener,
        target_accepts,
        &armed,
        &completed,
        &accepted_count,
        &error));

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
    for (int& client : clients) {
        client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        ASSERT_GE(client, 0);
        const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
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

TEST_F(UringIoRuntimeSocketFixture, IoUringRecvMultishotUsesProvidedBuffers) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    constexpr int target_reads = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringRecvMultishotTask>(
        fds[0],
        target_reads,
        &armed,
        &completed,
        &read_count,
        &packed_read,
        &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL ||
            setup_error == EOPNOTSUPP ||
            setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            close_pair(fds);
            GTEST_SKIP() << "io_uring provided buffer recv_multishot unsupported";
        }
        FAIL() << "recv_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'A', 'B'};
    ASSERT_EQ(::write(fds[1], payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL ||
        task_error == EOPNOTSUPP ||
        task_error == ENOSYS ||
        task_error == ENOBUFS) {
        close_pair(fds);
        GTEST_SKIP() << "io_uring provided buffer recv_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), target_reads);
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('A') << 8) | static_cast<int>('B'));

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringConnectedUdpRecvMultishotUsesProvidedBuffers) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in receiver_address{};
    receiver_address.sin_family = AF_INET;
    receiver_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    receiver_address.sin_port = 0;
    ASSERT_EQ(
        ::bind(receiver.get(), reinterpret_cast<sockaddr*>(&receiver_address), sizeof(receiver_address)),
        0);
    socklen_t receiver_address_size = sizeof(receiver_address);
    ASSERT_EQ(
        ::getsockname(
            receiver.get(),
            reinterpret_cast<sockaddr*>(&receiver_address),
            &receiver_address_size),
        0);

    sockaddr_in sender_address{};
    sender_address.sin_family = AF_INET;
    sender_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sender_address.sin_port = 0;
    ASSERT_EQ(
        ::bind(sender.get(), reinterpret_cast<sockaddr*>(&sender_address), sizeof(sender_address)),
        0);
    socklen_t sender_address_size = sizeof(sender_address);
    ASSERT_EQ(
        ::getsockname(
            sender.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            &sender_address_size),
        0);
    ASSERT_EQ(
        ::connect(
            receiver.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            sender_address_size),
        0);
    ASSERT_EQ(
        ::connect(
            sender.get(),
            reinterpret_cast<sockaddr*>(&receiver_address),
            receiver_address_size),
        0);

    constexpr int target_reads = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringRecvMultishotTask>(
        receiver.get(),
        target_reads,
        &armed,
        &completed,
        &read_count,
        &packed_read,
        &error,
        true));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL ||
            setup_error == EOPNOTSUPP ||
            setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
        }
        FAIL() << "UDP recv_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'U', 'D'};
    ASSERT_EQ(::send(sender.get(), payload, 1, 0), 1);
    ASSERT_EQ(::send(sender.get(), payload + 1, 1, 0), 1);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL ||
        task_error == EOPNOTSUPP ||
        task_error == ENOSYS ||
        task_error == ENOBUFS) {
        GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), target_reads);
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('U') << 8) | static_cast<int>('D'));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringUdpRecvmsgMultishotReportsPeerAddress) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in receiver_address{};
    receiver_address.sin_family = AF_INET;
    receiver_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    receiver_address.sin_port = 0;
    ASSERT_EQ(
        ::bind(receiver.get(), reinterpret_cast<sockaddr*>(&receiver_address), sizeof(receiver_address)),
        0);
    socklen_t receiver_address_size = sizeof(receiver_address);
    ASSERT_EQ(
        ::getsockname(
            receiver.get(),
            reinterpret_cast<sockaddr*>(&receiver_address),
            &receiver_address_size),
        0);

    sockaddr_in sender_address{};
    sender_address.sin_family = AF_INET;
    sender_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sender_address.sin_port = 0;
    ASSERT_EQ(
        ::bind(sender.get(), reinterpret_cast<sockaddr*>(&sender_address), sizeof(sender_address)),
        0);
    socklen_t sender_address_size = sizeof(sender_address);
    ASSERT_EQ(
        ::getsockname(
            sender.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            &sender_address_size),
        0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> peer_count{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpRecvmsgMultishotTask>(
        receiver.get(),
        sender_address.sin_port,
        &armed,
        &completed,
        &read_count,
        &packed_read,
        &peer_count,
        &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL ||
            setup_error == EOPNOTSUPP ||
            setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            GTEST_SKIP() << "io_uring UDP recvmsg_multishot unsupported";
        }
        FAIL() << "UDP recvmsg_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'R', 'M'};
    ASSERT_EQ(
        ::sendto(
            sender.get(),
            payload,
            1,
            0,
            reinterpret_cast<sockaddr*>(&receiver_address),
            receiver_address_size),
        1);
    ASSERT_EQ(
        ::sendto(
            sender.get(),
            payload + 1,
            1,
            0,
            reinterpret_cast<sockaddr*>(&receiver_address),
            receiver_address_size),
        1);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL ||
        task_error == EOPNOTSUPP ||
        task_error == ENOSYS ||
        task_error == ENOBUFS) {
        GTEST_SKIP() << "io_uring UDP recvmsg_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), 2);
    EXPECT_EQ(peer_count.load(std::memory_order_acquire), 2);
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('R') << 8) | static_cast<int>('M'));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadConnectsTcpStreamOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadReceivesUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpRecvTask>(
        receiver.get(),
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'g';
    ASSERT_EQ(::sendto(
                  sender.get(),
                  &value,
                  sizeof(value),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadReceivesVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredRecvTask>(
        receiver.get(),
        &armed,
        &completed,
        &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'q', 'r'};
    ASSERT_EQ(::sendto(
                  sender.get(),
                  payload,
                  sizeof(payload),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('q' << 8) | 'r');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadSendsUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 'm';
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpSendToTask>(
        sender.get(),
        address,
        address_size,
        value,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1);

    char received = 0;
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver.get(),
                  &received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              1);
    EXPECT_EQ(received, value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadSendsVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredSendToTask>(
        sender.get(),
        address,
        address_size,
        'x',
        'y',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver.get(),
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 'x');
    EXPECT_EQ(received[1], 'y');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketFixture, IoUringThreadSendsVectoredUdpDatagramWithSendmsgZcOrFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredZcSendToTask>(
        sender.get(),
        address,
        address_size,
        's',
        'z',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver.get(),
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 's');
    EXPECT_EQ(received[1], 'z');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
