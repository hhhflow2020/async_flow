#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketMultishotFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketMultishotFixture, IoUringConnectedUdpRecvMultishotUsesProvidedBuffers) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));
    ASSERT_TRUE(connect_udp_loopback_sockets(sockets));

    constexpr int target_reads = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringRecvMultishotTask>(
        sockets.receiver.get(), target_reads, &armed, &completed, &read_count, &packed_read, &error,
        true));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL || setup_error == EOPNOTSUPP || setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
        }
        FAIL() << "UDP recv_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'U', 'D'};
    ASSERT_EQ(::send(sockets.sender.get(), payload, 1, 0), 1);
    ASSERT_EQ(::send(sockets.sender.get(), payload + 1, 1, 0), 1);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL || task_error == EOPNOTSUPP || task_error == ENOSYS ||
        task_error == ENOBUFS) {
        GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), target_reads);
    EXPECT_EQ(packed_read.load(std::memory_order_acquire),
              (static_cast<int>('U') << 8) | static_cast<int>('D'));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketMultishotFixture, IoUringUdpRecvMultishotCancelDrainsQueuedCompletions) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));
    ASSERT_TRUE(connect_udp_loopback_sockets(sockets));

    constexpr int target_reads = 1;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringRecvMultishotTask>(
        sockets.receiver.get(), target_reads, &armed, &completed, &read_count, &packed_read, &error,
        true));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL || setup_error == EOPNOTSUPP || setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
        }
        FAIL() << "UDP recv_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'Q', 'R'};
    ASSERT_EQ(::send(sockets.sender.get(), payload, 1, 0), 1);
    ASSERT_EQ(::send(sockets.sender.get(), payload + 1, 1, 0), 1);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL || task_error == EOPNOTSUPP || task_error == ENOSYS ||
        task_error == ENOBUFS) {
        GTEST_SKIP() << "io_uring UDP recv_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), target_reads);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
