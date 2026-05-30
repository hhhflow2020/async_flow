#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

TEST_F(IoRuntimeEpollFixture, IoThreadUsesConfiguredThreadKindAndAcceptsTasks) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};

    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
}

TEST_F(IoRuntimeEpollFixture, WorkerThreadDoesNotExposeIoBackend) {
    EXPECT_FALSE(IoRuntime::io_backend_available(IoTestThread::Logic_0));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<WorkerIoWaitRejectedTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_NE(error.load(std::memory_order_acquire), 0);
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadResumesTaskWhenFdBecomesReadable) {
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

    const char value = 'x';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadRearmsReadableFdWithSameState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> reads{0};
    char output[2]{};

    ASSERT_TRUE(IoRuntime::start_task<SocketRepeatedReadableTask>(
        fds[0],
        &armed,
        &completed,
        &reads,
        output));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char first = 'a';
    ASSERT_EQ(::write(fds[1], &first, sizeof(first)), 1);
    ASSERT_TRUE(wait_until_at_least(reads, 1));
    ASSERT_TRUE(wait_until_at_least(armed, 2));

    const char second = 'b';
    ASSERT_EQ(::write(fds[1], &second, sizeof(second)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(output[0], first);
    EXPECT_EQ(output[1], second);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsPendingReadWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};

    ASSERT_TRUE(IoRuntime::start_task<CancellableSocketReadTask>(
        fds[0],
        &state,
        &armed,
        &read_completed,
        &read_error));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state,
        true,
        &cancel_completed,
        &first_cancel,
        &second_cancel,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(second_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadRejectsCancelForIdleState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> result{-1};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<CancelIdleIoStateTask>(&completed, &result, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(result.load(std::memory_order_acquire), 0);
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOENT);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadTimesOutPendingRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::milliseconds(1),
        &state,
        &armed,
        &completed,
        &error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ETIMEDOUT);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsTimeoutWhenReadCompletes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::seconds(1),
        &state,
        &armed,
        &completed,
        &error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 't';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadCancelsTimeoutWhenIoIsCanceled) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::milliseconds(20),
        &state,
        &armed,
        &read_completed,
        &read_error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state,
        false,
        &cancel_completed,
        &first_cancel,
        &second_cancel,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, IoHelpersHandleInvalidAndZeroByteOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> bad_fd_completed{0};
    std::atomic<int> bad_fd_error{0};
    ASSERT_TRUE(IoRuntime::start_task<BadFdReadTask>(&bad_fd_completed, &bad_fd_error));
    ASSERT_TRUE(wait_until_at_least(bad_fd_completed, 1));
    EXPECT_EQ(bad_fd_error.load(std::memory_order_acquire), EBADF);

    std::atomic<int> zero_completed{0};
    ASSERT_TRUE(IoRuntime::start_task<ZeroByteIoTask>(&zero_completed));
    ASSERT_TRUE(wait_until_at_least(zero_completed, 1));
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, ZeroCopyHelpersHandleInvalidAndZeroCountOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<ZeroCopyBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "zero-copy helpers are Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, FileAdapterHandlesInvalidAndZeroByteOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<FileAdapterBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, FixedBufferHelpersHandleInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedBufferBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, FixedFileHelpersHandleInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> register_error{0};
    std::atomic<int> unavailable_error{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedFileBoundaryTask>(
        &completed,
        &register_error,
        &unavailable_error,
        &invalid_error,
        &null_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(register_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, VectoredHelpersHandleInvalidAndZeroLengthOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<VectoredBoundaryTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, TimerFdAdapterHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int error = 0;
    EXPECT_FALSE(af::arm_timerfd_after(-1, std::chrono::milliseconds(1), error));
    EXPECT_EQ(error, EBADF);
    EXPECT_FALSE(af::arm_timerfd_after(0, std::chrono::nanoseconds{0}, error));
    EXPECT_EQ(error, EINVAL);

    std::atomic<int> completed{0};
    std::atomic<int> task_error{0};
    ASSERT_TRUE(IoRuntime::start_task<TimerBoundaryTask>(&completed, &task_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(task_error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, TimeoutHelperHandlesInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> task_error{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutBoundaryTask>(&completed, &task_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(task_error.load(std::memory_order_acquire), ENOSYS);
#else
    GTEST_SKIP() << "io_uring timeout helper is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EventFdAdapterHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int error = 0;
    EXPECT_FALSE(af::write_eventfd(-1, 1, error));
    EXPECT_EQ(error, EBADF);

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);
    EXPECT_FALSE(af::write_eventfd(
        event.get(),
        std::numeric_limits<std::uint64_t>::max(),
        error));
    EXPECT_EQ(error, EINVAL);

    std::atomic<int> completed{0};
    std::atomic<int> task_error{0};
    ASSERT_TRUE(IoRuntime::start_task<EventBoundaryTask>(&completed, &task_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(task_error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, OpenAtHelperHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<OpenAtBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOSYS);
#else
    GTEST_SKIP() << "openat helper is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, SocketLifecycleHelpersRunOnIoThread) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<int> reuse_value{0};
    std::atomic<int> local_port{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};
    ASSERT_TRUE(IoRuntime::start_task<SocketLifecycleSetupTask>(
        &completed,
        &error,
        &reuse_value,
        &local_port,
        &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_NE(reuse_value.load(std::memory_order_acquire), 0);
    EXPECT_GT(local_port.load(std::memory_order_acquire), 0);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
#else
    GTEST_SKIP() << "socket lifecycle helpers are Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, SocketLifecycleHelpersHandleInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    ASSERT_TRUE(IoRuntime::start_task<SocketLifecycleBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
#else
    GTEST_SKIP() << "socket lifecycle helpers are Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadResumesTimerFdFromAdapter) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    af::UniqueFd timer = af::make_timerfd();
    ASSERT_TRUE(timer);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> expirations{0};
    ASSERT_TRUE(IoRuntime::start_task<TimerFdTask>(
        timer.get(),
        &armed,
        &completed,
        &expirations));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::arm_timerfd_after(timer.get(), std::chrono::milliseconds(1), error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_GE(expirations.load(std::memory_order_acquire), std::uint64_t{1});
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(IoRuntimeEpollFixture, EpollIoThreadResumesEventFdFromAdapter) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> value{0};
    ASSERT_TRUE(IoRuntime::start_task<EventFdTask>(
        event.get(),
        &armed,
        &completed,
        &value));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::write_eventfd(event.get(), 7, error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(value.load(std::memory_order_acquire), std::uint64_t{7});
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}
