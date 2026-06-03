#include "runtime_io_test_support.hpp"

class IoRuntimeDatagramFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeDatagramFixture, NativeIoThreadSendsUdpDatagramFromHelper) {
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "native IO backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 's';
    ASSERT_TRUE(IoRuntime::start_task<UdpSendToTask>(sockets.sender.get(), sockets.address,
                                                     sockets.address_size, value, &completed,
                                                     &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1);

    char received = 0;
    ASSERT_EQ(recv_udp_payload(sockets, &received, sizeof(received)), 1);
    EXPECT_EQ(received, value);
}

TEST_F(IoRuntimeDatagramFixture, NativeIoThreadSendsVectoredUdpDatagramFromHelper) {
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "native IO backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredSendToTask>(sockets.sender.get(), sockets.address,
                                                             sockets.address_size, 'd', 'g',
                                                             &completed, &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    ASSERT_EQ(recv_udp_payload(sockets, received, sizeof(received)), 2);
    EXPECT_EQ(received[0], 'd');
    EXPECT_EQ(received[1], 'g');
}

TEST_F(IoRuntimeDatagramFixture, NativeIoThreadSendsVectoredUdpDatagramWithSendmsgZcHelper) {
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "native IO backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredZcSendToTask>(
        sockets.sender.get(), sockets.address, sockets.address_size, 'z', 'c', &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    ASSERT_EQ(recv_udp_payload(sockets, received, sizeof(received)), 2);
    EXPECT_EQ(received[0], 'z');
    EXPECT_EQ(received[1], 'c');
}
