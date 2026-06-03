#include "runtime_io_test_support.hpp"

class NonLinuxUringReadFallbackRejectTask final : public UringIoTaskBase {
public:
    explicit NonLinuxUringReadFallbackRejectTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *fd, std::atomic<int> *error,
               std::atomic<std::int64_t> *result_value, std::atomic<int> *token_cleared) {
        completed_ = completed;
        fd_ = fd;
        error_ = error;
        result_value_ = result_value;
        token_cleared_ = token_cleared;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        int token = 0;
        char buffer = 0;
        af::IoResult result{42, af::io_readable, 0, 17, &token};
        const bool accepted = UringIoRuntime::io_submit_read_at(IoTestThreads::IO_0, 0, &buffer,
                                                                sizeof(buffer), 0, this, &result);
        if (accepted) {
            return failed();
        }

        fd_->store(result.fd, std::memory_order_release);
        error_->store(result.error, std::memory_order_release);
        result_value_->store(result.result, std::memory_order_release);
        token_cleared_->store(result.completion_token == nullptr ? 1 : 0,
                              std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *fd_{nullptr};
    std::atomic<int> *error_{nullptr};
    std::atomic<std::int64_t> *result_value_{nullptr};
    std::atomic<int> *token_cleared_{nullptr};
};

class UringIoRuntimeFileFixture : public UringIoRuntimeFixture {};

TEST_F(UringIoRuntimeFileFixture, NonLinuxIoUringReadFallbackClearsStaleResultState) {
#if !defined(__linux__)
    std::atomic<int> completed{0};
    std::atomic<int> fd{-1};
    std::atomic<int> error{0};
    std::atomic<std::int64_t> result_value{0};
    std::atomic<int> token_cleared{0};
    ASSERT_TRUE(UringIoRuntime::start_task<NonLinuxUringReadFallbackRejectTask>(
        &completed, &fd, &error, &result_value, &token_cleared));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(fd.load(std::memory_order_acquire), 0);
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(result_value.load(std::memory_order_acquire), -ENOSYS);
    EXPECT_EQ(token_cleared.load(std::memory_order_acquire), 1);
#else
    GTEST_SKIP() << "non-Linux io_uring fallback is not used on Linux";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringFileAdapterWritesFsyncsAndReadsAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(
        UringIoRuntime::start_task<UringFileReadWriteTask>(file.get(), &completed, &byte_read));
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
    if (!UringIoRuntime::io_uring_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-vectored-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileVectoredReadWriteTask>(file.get(), &completed,
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
    if (!UringIoRuntime::io_uring_backend_available(IoTestThreads::IO_0)) {
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
        file.get(), &completed, &packed_read, &pending_submits));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(packed_read.load(std::memory_order_acquire),
              (static_cast<int>('A') << 16) | (static_cast<int>('B') << 8) | static_cast<int>('C'));
    EXPECT_GE(pending_submits.load(std::memory_order_acquire), 4);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFileFixture, IoUringOversizedReadRejectClearsStaleResultState) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThreads::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-oversized-read-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<std::int64_t> result_value{0};
    std::atomic<int> token_cleared{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringOversizedReadRejectTask>(
        file.get(), &completed, &error, &result_value, &token_cleared));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(result_value.load(std::memory_order_acquire), -EINVAL);
    EXPECT_EQ(token_cleared.load(std::memory_order_acquire), 1);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}
