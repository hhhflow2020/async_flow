#include "runtime_io_test_support.hpp"

class UringIoRuntimeFileFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeFileFixture, IoUringRegisteredBufferReadsAndWritesAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-fixed-buffer-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(
        UringIoRuntime::start_task<UringFixedBufferFileTask>(file.get(), &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'B');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFixedFileWritesFsyncsAndReadsAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-fixed-file-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileTask>(file.get(), &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'F');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFixedFileTableUpdatesRegisteredSlot) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char first_path[] = "/tmp/asyncflow-uring-fixed-update-a-XXXXXX";
    const int first_fd = ::mkstemp(first_path);
    ASSERT_GE(first_fd, 0);
    af::UniqueFd first(first_fd);
    char second_path[] = "/tmp/asyncflow-uring-fixed-update-b-XXXXXX";
    const int second_fd = ::mkstemp(second_path);
    ASSERT_GE(second_fd, 0);
    af::UniqueFd second(second_fd);

    const char first_payload = '1';
    const char second_payload = '2';
    ASSERT_EQ(::write(first.get(), &first_payload, sizeof(first_payload)), 1);
    ASSERT_EQ(::write(second.get(), &second_payload, sizeof(second_payload)), 1);

    std::atomic<int> completed{0};
    std::atomic<int> packed_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileUpdateTask>(first.get(), second.get(),
                                                                     &completed, &packed_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(packed_read.load(std::memory_order_acquire),
              (static_cast<int>('1') << 8) | static_cast<int>('2'));

    first.reset();
    second.reset();
    static_cast<void>(::unlink(first_path));
    static_cast<void>(::unlink(second_path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringOpenAtDirectInstallsFixedFileSlot) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-openat-direct-XXXXXX";
    int seed = ::mkstemp(path);
    ASSERT_GE(seed, 0);
    close_fd(seed);
    static_cast<void>(::unlink(path));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringOpenAtDirectFileTask>(path, &completed, &error,
                                                                      &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    const int direct_error = error.load(std::memory_order_acquire);
    if (direct_error == EINVAL || direct_error == EBADF || direct_error == ENOSYS
#ifdef EOPNOTSUPP
        || direct_error == EOPNOTSUPP
#endif
#ifdef ENXIO
        || direct_error == ENXIO
#endif
    ) {
        static_cast<void>(::unlink(path));
        GTEST_SKIP() << "io_uring direct descriptor open unsupported: " << direct_error;
    }

    EXPECT_EQ(direct_error, 0);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'D');

    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring direct descriptor open is Linux-only";
#endif
}
