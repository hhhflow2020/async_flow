#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketDatagramFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadReceivesUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpRecvTask>(sockets.receiver.get(), &armed,
                                                             &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'g';
    ASSERT_EQ(send_udp_payload(sockets, &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketDatagramFixture,
       IoUringThreadReceivesVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredRecvTask>(sockets.receiver.get(), &armed,
                                                                     &completed, &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'q', 'r'};
    ASSERT_EQ(send_udp_payload(sockets, payload, sizeof(payload)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('q' << 8) | 'r');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadSendsUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 'm';
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpSendToTask>(
        sockets.sender.get(), sockets.address, sockets.address_size, value, &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1);

    char received = 0;
    ASSERT_EQ(recv_udp_payload(sockets, &received, sizeof(received)), 1);
    EXPECT_EQ(received, value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketDatagramFixture,
       IoUringThreadSendsVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredSendToTask>(
        sockets.sender.get(), sockets.address, sockets.address_size, 'x', 'y', &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    ASSERT_EQ(recv_udp_payload(sockets, received, sizeof(received)), 2);
    EXPECT_EQ(received[0], 'x');
    EXPECT_EQ(received[1], 'y');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketDatagramFixture,
       IoUringThreadSendsVectoredUdpDatagramWithSendmsgZcOrFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredZcSendToTask>(
        sockets.sender.get(), sockets.address, sockets.address_size, 's', 'z', &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    ASSERT_EQ(recv_udp_payload(sockets, received, sizeof(received)), 2);
    EXPECT_EQ(received[0], 's');
    EXPECT_EQ(received[1], 'z');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
