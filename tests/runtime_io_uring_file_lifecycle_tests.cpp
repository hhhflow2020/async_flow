#include "runtime_io_test_support.hpp"

class UringIoRuntimeFileFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeFileFixture, IoUringThreadOpensFileWithOpenAt) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-openat-XXXXXX";
    int seed = ::mkstemp(path);
    ASSERT_GE(seed, 0);
    close_fd(seed);
    static_cast<void>(::unlink(path));

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringOpenAtFileTask>(path, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'O');

    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFileLifecycleRunsOnIoThread) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-lifecycle-XXXXXX";
    int seed = ::mkstemp(path);
    ASSERT_GE(seed, 0);
    close_fd(seed);
    static_cast<void>(::unlink(path));

    char renamed_path[sizeof(path) + 8]{};
    ASSERT_GT(std::snprintf(renamed_path, sizeof(renamed_path), "%s.renamed", path), 0);
    static_cast<void>(::unlink(renamed_path));

    std::atomic<int> completed{0};
    std::atomic<int> close_released{0};
    std::atomic<std::uint64_t> observed_size{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileLifecycleTask>(
        path, renamed_path, &completed, &close_released, &observed_size));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(close_released.load(std::memory_order_acquire), 1);
    EXPECT_EQ(observed_size.load(std::memory_order_acquire), std::uint64_t{1});
    EXPECT_EQ(::access(path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
    errno = 0;
    EXPECT_EQ(::access(renamed_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);

    static_cast<void>(::unlink(path));
    static_cast<void>(::unlink(renamed_path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFilesystemOpsRunOnIoThread) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char dir_path[] = "/tmp/asyncflow-fsops-XXXXXX";
    ASSERT_NE(::mkdtemp(dir_path), nullptr);
    ASSERT_EQ(::rmdir(dir_path), 0);

    char file_path[sizeof(dir_path) + 8]{};
    char hardlink_path[sizeof(dir_path) + 12]{};
    char symlink_path[sizeof(dir_path) + 12]{};
    ASSERT_GT(std::snprintf(file_path, sizeof(file_path), "%s/file", dir_path), 0);
    ASSERT_GT(std::snprintf(hardlink_path, sizeof(hardlink_path), "%s/hard", dir_path), 0);
    ASSERT_GT(std::snprintf(symlink_path, sizeof(symlink_path), "%s/sym", dir_path), 0);
    static_cast<void>(::unlink(file_path));
    static_cast<void>(::unlink(hardlink_path));
    static_cast<void>(::unlink(symlink_path));
    static_cast<void>(::rmdir(dir_path));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<std::uint64_t> observed_size{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFilesystemOpsTask>(
        dir_path, file_path, hardlink_path, symlink_path, &completed, &error, &observed_size));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL || task_error == EOPNOTSUPP || task_error == ENOSYS) {
        static_cast<void>(::unlink(file_path));
        static_cast<void>(::unlink(hardlink_path));
        static_cast<void>(::unlink(symlink_path));
        static_cast<void>(::rmdir(dir_path));
        GTEST_SKIP() << "kernel does not support one of the requested io_uring fs opcodes";
    }

    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(observed_size.load(std::memory_order_acquire), std::uint64_t{1});
    EXPECT_EQ(::access(file_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
    errno = 0;
    EXPECT_EQ(::access(hardlink_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
    errno = 0;
    EXPECT_EQ(::access(symlink_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
    errno = 0;
    EXPECT_EQ(::access(dir_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);

    static_cast<void>(::unlink(file_path));
    static_cast<void>(::unlink(hardlink_path));
    static_cast<void>(::unlink(symlink_path));
    static_cast<void>(::rmdir(dir_path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
