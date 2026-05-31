#include "runtime_io_test_support.hpp"

class UringIoRuntimeSocketCoreFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeSocketCoreFixture, IoUringThreadFallsBackToEpollReadiness) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamFallbackTask>(
        fds[0],
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'U';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketCoreFixture, IoUringTimeoutCompletesInRing) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTimeoutTask>(&armed, &completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    if (error.load(std::memory_order_acquire) == ENOSYS ||
        error.load(std::memory_order_acquire) == EINVAL) {
        GTEST_SKIP() << "io_uring timeout operation unsupported";
    }
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_GE(armed.load(std::memory_order_acquire), 1);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketCoreFixture, IoUringThreadCancelsPendingRecvCompletion) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> armed{0};
    std::atomic<int> recv_completed{0};
    std::atomic<int> recv_error{0};
    std::atomic<int> recv_bytes{-1};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancellableSocketRecvTask>(
        fds[0],
        &state,
        &wait_kind,
        &armed,
        &recv_completed,
        &recv_error,
        &recv_bytes));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    if (wait_kind.load(std::memory_order_acquire) !=
        static_cast<int>(af::IoWaitKind::Completion)) {
        const char value = 'f';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
        close_pair(fds);
        GTEST_SKIP() << "recv did not remain as an io_uring completion operation";
    }

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(
        &state,
        &cancel_completed,
        &cancel_result,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));

    if (!wait_until_at_least(recv_completed, 1)) {
        const char value = 'u';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
    }

    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_bytes.load(std::memory_order_acquire), -1);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeSocketCoreFixture, IoUringThreadHandlesTimerFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd timer = af::make_timerfd();
    ASSERT_TRUE(timer);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> expirations{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTimerFdTask>(
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

TEST_F(UringIoRuntimeSocketCoreFixture, IoUringThreadHandlesEventFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> value{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringEventFdTask>(
        event.get(),
        &armed,
        &completed,
        &value));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::write_eventfd(event.get(), 9, error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(value.load(std::memory_order_acquire), std::uint64_t{9});
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}
