#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketDatagramFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadReceivesUdpDatagramOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadReceivesVectoredUdpDatagramOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadSendsUdpDatagramOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadSendsVectoredUdpDatagramOrFallsBackToEpoll) {
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

TEST_F(UringIoRuntimeSocketDatagramFixture, IoUringThreadSendsVectoredUdpDatagramWithSendmsgZcOrFallback) {
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
