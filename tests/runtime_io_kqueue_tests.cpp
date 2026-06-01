#include "runtime_io_test_support.hpp"

#if AF_DETAIL_HAS_KQUEUE
namespace {

bool set_nonblocking_cloexec(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return false;
    }
    const int fd_flags = ::fcntl(fd, F_GETFD, 0);
    return fd_flags >= 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
}

bool make_socket_pair(int fds[2]) {
    fds[0] = -1;
    fds[1] = -1;
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return false;
    }
    if (set_nonblocking_cloexec(fds[0]) && set_nonblocking_cloexec(fds[1])) {
        return true;
    }
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
    fds[0] = -1;
    fds[1] = -1;
    return false;
}

void close_pair_local(int fds[2]) {
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
}

} // namespace

class IoRuntimeKqueueFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeKqueueFixture, NativeIoThreadUsesKqueueBackendAndAcceptsTasks) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThread::IO_0));
    EXPECT_FALSE(IoRuntime::io_backend_available(IoTestThread::Logic_0));

    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};
    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadResumesTaskWhenFdBecomesReadable) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThread::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};

    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'k';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadCancelsPendingReadWait) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThread::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};

    ASSERT_TRUE(IoRuntime::start_task<CancellableSocketReadTask>(fds[0], &state, &armed,
                                                                 &read_completed, &read_error));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state, true, &cancel_completed, &first_cancel, &second_cancel, &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(second_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadTimesOutPendingRead) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThread::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};

    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0], std::chrono::milliseconds(2), &state, &armed, &completed, &error, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ETIMEDOUT);

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueTimeoutIsCanceledWhenReadCompletesFirst) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThread::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

    std::atomic<af::IoOpState *> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};

    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0], std::chrono::seconds(5), &state, &armed, &completed, &error, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 't';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair_local(fds);
}
#else
TEST(IoRuntimeKqueue, KqueueBackendIsPlatformSpecific) {
    GTEST_SKIP() << "kqueue backend is only built on macOS/BSD platforms";
}
#endif
