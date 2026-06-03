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

class KqueueShutdownWriteTask final : public IoTaskBase {
public:
    explicit KqueueShutdownWriteTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<int> *error) {
        fd_ = fd;
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status =
            af::io_shutdown(*this, IoTestThreads::IO_0, fd_, SHUT_WR, shutdown_);
        if (status.pending()) {
            return pending();
        }
        error_->store(status.ready() ? 0 : status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    af::IoOpState shutdown_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};

bool fill_until_blocked_local(int fd) {
    char data[4096]{};
    bool blocked = false;
    for (;;) {
        const ssize_t n = ::write(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            blocked = true;
        }
        break;
    }
    return blocked;
}

void drain_available_local(int fd) {
    char data[4096]{};
    for (;;) {
        const ssize_t n = ::read(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

} // namespace

class IoRuntimeKqueueFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeKqueueFixture, NativeIoThreadUsesKqueueBackendAndAcceptsTasks) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));
    EXPECT_FALSE(IoRuntime::io_backend_available(IoTestThreads::Logic_0));

    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};
    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThreads::IO_0));
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadResumesTaskWhenFdBecomesReadable) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

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

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadRejectsDuplicateReadWait) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

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

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadAllowsSameFdReadAndWriteWaits) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));
    ASSERT_TRUE(fill_until_blocked_local(fds[0]));

    std::atomic<int> read_armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &read_armed, &read_completed,
                                                          &byte_read));
    ASSERT_TRUE(wait_until_at_least(read_armed, 1));

    std::atomic<int> write_armed{0};
    std::atomic<int> write_completed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketWritableTask>(fds[0], &write_armed, &write_completed));
    ASSERT_TRUE(wait_until_at_least(write_armed, 1));

    const char value = 'r';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    drain_available_local(fds[1]);
    ASSERT_TRUE(wait_until_at_least(write_completed, 1));

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadCancelsPendingReadWait) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

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
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

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

TEST_F(IoRuntimeKqueueFixture, KqueueIoThreadShutdownWriteHalfClosesPeer) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

    int fds[2]{-1, -1};
    ASSERT_TRUE(make_socket_pair(fds));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<KqueueShutdownWriteTask>(fds[0], &completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);

    char ignored = 0;
    EXPECT_EQ(::read(fds[1], &ignored, sizeof(ignored)), 0);

    close_pair_local(fds);
}

TEST_F(IoRuntimeKqueueFixture, KqueueTimeoutIsCanceledWhenReadCompletesFirst) {
    ASSERT_TRUE(IoRuntime::io_backend_available(IoTestThreads::IO_0));

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
