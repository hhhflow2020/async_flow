#include "runtime_io_test_support.hpp"

class IoRuntimeDatagramFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadRejectsDuplicateFdWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> rejected{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<DuplicateWaitRejectedTask>(fds[0], &rejected, &error));
    ASSERT_TRUE(wait_until_at_least(rejected, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EALREADY);

    const char value = 'd';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadResumesTaskWhenFdBecomesWritable) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketWritableTask>(fds[0], &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadResumesUdpRecvFromHelper) {
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

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpRecvTask>(receiver, &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'u';
    ASSERT_EQ(::sendto(
                  sender,
                  &value,
                  sizeof(value),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadReceivesVectoredUdpDatagramFromHelper) {
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

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredRecvTask>(
        receiver,
        &armed,
        &completed,
        &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'u', 'v'};
    ASSERT_EQ(::sendto(
                  sender,
                  payload,
                  sizeof(payload),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('u' << 8) | 'v');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadAcceptsUdpZeroLengthDatagram) {
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

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{'z'};

    const char value = 0;
    ASSERT_EQ(::sendto(
                  sender,
                  &value,
                  0,
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              0);

    ASSERT_TRUE(IoRuntime::start_task<UdpRecvTask>(receiver, &armed, &completed, &byte_read, 0U));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'z');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

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

TEST_F(IoRuntimeDatagramFixture, EpollIoThreadReportsPeerHangupAsClosedRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> closed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketHangupTask>(fds[0], &armed, &closed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ::close(fds[1]);
    fds[1] = -1;
    ASSERT_TRUE(wait_until_at_least(closed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
