#include "runtime_io_test_support.hpp"

class IoRuntimeEpollFixture : public IoRuntimeFixture {};

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
    std::atomic<int> resource_unavailable_error{0};
    std::atomic<int> resource_invalid_error{0};
    std::atomic<int> resource_null_error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedFileResourceBoundaryTask>(
        &completed, &register_error, &resource_unavailable_error, &resource_invalid_error,
        &resource_null_error));

    std::atomic<int> accept_bad_fd_error{0};
    std::atomic<int> accept_null_output_error{0};
    std::atomic<int> accept_bad_address_error{0};
    std::atomic<int> accept_bad_index_error{0};
    std::atomic<int> accept_unavailable_error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedFileAcceptDirectBoundaryTask>(
        &completed, &accept_bad_fd_error, &accept_null_output_error, &accept_bad_address_error,
        &accept_bad_index_error, &accept_unavailable_error));

    std::atomic<int> data_unavailable_error{0};
    std::atomic<int> data_invalid_error{0};
    std::atomic<int> data_null_error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedFileDataBoundaryTask>(
        &completed, &data_unavailable_error, &data_invalid_error, &data_null_error));

    ASSERT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_EQ(register_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(resource_unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(resource_invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(resource_null_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(accept_bad_fd_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(accept_null_output_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(accept_bad_address_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(accept_bad_index_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(accept_unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(data_unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(data_invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(data_null_error.load(std::memory_order_acquire), EINVAL);
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
    EXPECT_FALSE(af::write_eventfd(event.get(), std::numeric_limits<std::uint64_t>::max(), error));
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
    std::atomic<int> open_error{0};
    std::atomic<int> metadata_error{0};
    std::atomic<int> namespace_error{0};
    ASSERT_TRUE(IoRuntime::start_task<OpenAtBoundaryTask>(&completed, &open_error));
    ASSERT_TRUE(IoRuntime::start_task<FilesystemMetadataBoundaryTask>(&completed, &metadata_error));
    ASSERT_TRUE(
        IoRuntime::start_task<FilesystemNamespaceBoundaryTask>(&completed, &namespace_error));
    ASSERT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_EQ(open_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(metadata_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(namespace_error.load(std::memory_order_acquire), ENOSYS);
#else
    GTEST_SKIP() << "openat helper is Linux-only";
#endif
}
