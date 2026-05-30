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
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedBufferFileTask>(
        file.get(),
        &completed,
        &byte_read));
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
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileTask>(
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
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileUpdateTask>(
        first.get(),
        second.get(),
        &completed,
        &packed_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
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
    ASSERT_TRUE(UringIoRuntime::start_task<UringOpenAtDirectFileTask>(
        path,
        &completed,
        &error,
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

TEST_F(UringIoRuntimeFileFixture, IoUringBatchedSubmitCompletesBurstWrites) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-batch-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    constexpr int write_count = 16;
    std::atomic<int> completed{0};
    for (int i = 0; i < write_count; ++i) {
        ASSERT_TRUE(UringIoRuntime::start_task<UringBatchedFileWriteTask>(
            file.get(),
            static_cast<std::uint64_t>(i),
            static_cast<char>('a' + i),
            &completed));
    }

    ASSERT_TRUE(wait_until_at_least(completed, write_count));

    char observed[write_count]{};
    ASSERT_EQ(::pread(file.get(), observed, sizeof(observed), 0), write_count);
    for (int i = 0; i < write_count; ++i) {
        EXPECT_EQ(observed[i], static_cast<char>('a' + i));
    }

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

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
    ASSERT_TRUE(UringIoRuntime::start_task<UringOpenAtFileTask>(
        path,
        &completed,
        &byte_read));
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
        path,
        renamed_path,
        &completed,
        &close_released,
        &observed_size));
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
        dir_path,
        file_path,
        hardlink_path,
        symlink_path,
        &completed,
        &error,
        &observed_size));
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
