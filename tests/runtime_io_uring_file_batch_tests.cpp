#include "runtime_io_test_support.hpp"

class UringIoRuntimeFileFixture : public UringIoRuntimeFixture {};

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
