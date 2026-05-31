#include "runtime_io_test_support.hpp"

class IoRuntimeDatagramFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadSendsUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 's';
    ASSERT_TRUE(IoRuntime::start_task<UdpSendToTask>(
        sender,
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
                  receiver,
                  &received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              1);
    EXPECT_EQ(received, value);

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadSendsVectoredUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredSendToTask>(
        sender,
        address,
        address_size,
        'd',
        'g',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver,
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 'd');
    EXPECT_EQ(received[1], 'g');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadSendsVectoredUdpDatagramWithSendmsgZcHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredZcSendToTask>(
        sender,
        address,
        address_size,
        'z',
        'c',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver,
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 'z');
    EXPECT_EQ(received[1], 'c');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "sendmsg_zc helper is Linux-only";
#endif
}
