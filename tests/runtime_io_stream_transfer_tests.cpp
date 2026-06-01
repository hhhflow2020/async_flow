#include "runtime_io_test_support.hpp"

class IoRuntimeStreamFixture : public IoRuntimeFixture {};
class UringIoRuntimePollFixture : public UringIoRuntimeFixture {};

TEST_F(IoRuntimeStreamFixture, StreamAdapterSendfileSendsFileToSocket) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    const char payload[] = "asyncflow-sendfile";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    af::UniqueFd file;
    ASSERT_TRUE(create_temp_file_with_payload(file, "asyncflow-sendfile", payload, payload_size));

    StreamSocketPair sockets;
    ASSERT_TRUE(create_stream_socket_pair(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendfileSocketTask>(
        sockets.first.get(), file.get(), payload_size, 3, true, &completed, &calls, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);
    EXPECT_GT(calls.load(std::memory_order_acquire), 1);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(sockets.second.get(), received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, SendfileWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    const char payload = 'P';
    af::UniqueFd file;
    ASSERT_TRUE(create_temp_file_with_payload(file, "asyncflow-sendfile-pending", &payload,
                                              sizeof(payload)));

    BlockingTcpConnection connection;
    ASSERT_TRUE(create_blocked_tcp_connection(connection));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendfileTask>(connection.server.get(), file.get(),
                                                           &pending_seen, &completed, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(connection.client.get());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(UringIoRuntimePollFixture, IoUringPollReadinessResumesSendfileWhenSocketWritable) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    const char payload = 'R';
    af::UniqueFd file;
    ASSERT_TRUE(create_temp_file_with_payload(file, "asyncflow-uring-poll-sendfile", &payload,
                                              sizeof(payload)));

    BlockingTcpConnection connection;
    ASSERT_TRUE(create_blocked_tcp_connection(connection));

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        connection.server.get(), file.get(), &state, &wait_kind, &pending_seen, &completed, &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    EXPECT_EQ(wait_kind.load(std::memory_order_acquire),
              static_cast<int>(af::IoWaitKind::Readiness));

    drain_available(connection.client.get());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimePollFixture, IoUringPollReadinessCancelPendingSendfileWait) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    const char payload = 'C';
    af::UniqueFd file;
    ASSERT_TRUE(create_temp_file_with_payload(file, "asyncflow-uring-poll-cancel", &payload,
                                              sizeof(payload)));

    BlockingTcpConnection connection;
    ASSERT_TRUE(create_blocked_tcp_connection(connection));

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        connection.server.get(), file.get(), &state, &wait_kind, &pending_seen, &completed, &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    ASSERT_EQ(wait_kind.load(std::memory_order_acquire),
              static_cast<int>(af::IoWaitKind::Readiness));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(&state, &cancel_completed,
                                                                   &cancel_result, &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 0U);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeStreamFixture, SpliceTransfersPipeContentWithNullOffsets) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    PipePair input;
    PipePair output;
    ASSERT_TRUE(create_pipe_pair(input));
    ASSERT_TRUE(create_pipe_pair(output));

    const char payload[] = "splice-through-kernel";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    ASSERT_TRUE(write_fd_all(input.write.get(), payload, payload_size));

    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_spliced{0};
    ASSERT_TRUE(IoRuntime::start_task<SplicePipeTask>(input.read.get(), output.write.get(),
                                                      payload_size, &completed, &bytes_spliced));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_spliced.load(std::memory_order_acquire), payload_size);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(output.read.get(), received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);
#else
    GTEST_SKIP() << "splice is Linux-only";
#endif
}
