#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketMultishotFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketMultishotFixture, IoUringConnectedUdpRecvMultishotUsesProvidedBuffers) {
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
