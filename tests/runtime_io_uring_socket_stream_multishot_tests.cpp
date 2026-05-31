#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketMultishotFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketMultishotFixture, IoUringRecvMultishotUsesProvidedBuffers) {
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
