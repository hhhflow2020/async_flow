#include "runtime_io_test_support.hpp"

class IoRuntimeDatagramFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadResumesUdpRecvFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(
        IoRuntime::start_task<UdpRecvTask>(sockets.receiver.get(), &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'u';
    ASSERT_EQ(send_udp_payload(sockets, &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadReceivesVectoredUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredRecvTask>(sockets.receiver.get(), &armed,
                                                           &completed, &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'u', 'v'};
    ASSERT_EQ(send_udp_payload(sockets, payload, sizeof(payload)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('u' << 8) | 'v');
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadAcceptsUdpZeroLengthDatagram) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    UdpLoopbackSockets sockets;
    ASSERT_TRUE(create_udp_loopback_sockets(sockets));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{'z'};

    const char value = 0;
    ASSERT_EQ(send_udp_payload(sockets, &value, 0), 0);

    ASSERT_TRUE(IoRuntime::start_task<UdpRecvTask>(sockets.receiver.get(), &armed, &completed,
                                                   &byte_read, 0U));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'z');
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
