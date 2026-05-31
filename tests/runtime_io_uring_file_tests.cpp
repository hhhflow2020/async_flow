#include "runtime_io_test_support.hpp"

class UringIoRuntimeFileFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeFileFixture, IoUringFileAdapterWritesFsyncsAndReadsAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileReadWriteTask>(
        file.get(),
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'F');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFileAdapterWritesAndReadsVectoredAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-vectored-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileVectoredReadWriteTask>(
        file.get(),
        &completed,
        &bytes_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_read.load(std::memory_order_acquire), 2);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFileAdapterUsesAsyncCurrentOffsetReadWrite) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-current-offset-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> pending_submits{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileCurrentOffsetTask>(
        file.get(),
        &completed,
        &packed_read,
        &pending_submits));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('A') << 16) | (static_cast<int>('B') << 8) | static_cast<int>('C'));
    EXPECT_GE(pending_submits.load(std::memory_order_acquire), 4);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
