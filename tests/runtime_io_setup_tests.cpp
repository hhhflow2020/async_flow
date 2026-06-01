#include "runtime_io_test_support.hpp"

TEST(IoUringSetupConfig, PopulatesRequestedSetupParams) {
#if defined(__linux__)
    io_uring_params params{};
    af::detail::configure_io_uring_params(
        params, af::detail::IoUringSetupRequest{
                    IORING_SETUP_SQPOLL | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_COOP_TASKRUN |
                        IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN,
                    2048U, 2500U, 3});

    EXPECT_NE(params.flags & IORING_SETUP_SQPOLL, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_SQ_AFF, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_CQSIZE, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_SUBMIT_ALL, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_COOP_TASKRUN, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_SINGLE_ISSUER, 0U);
    EXPECT_NE(params.flags & IORING_SETUP_DEFER_TASKRUN, 0U);
    EXPECT_EQ(params.cq_entries, 2048U);
    EXPECT_EQ(params.sq_thread_idle, 2500U);
    EXPECT_EQ(params.sq_thread_cpu, 3U);
#else
    GTEST_SKIP() << "io_uring setup params are Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringBackendAvailabilityReportsSetupError) {
    const bool available = UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0);
    const int error = UringIoRuntime::io_uring_backend_error(IoTestThread::IO_0);
#if defined(__linux__)
    if (available) {
        EXPECT_EQ(error, 0);
        return;
    }
    ASSERT_NE(error, 0);
    GTEST_SKIP() << "io_uring backend unavailable, error=" << error << " (" << std::strerror(error)
                 << ")";
#else
    EXPECT_FALSE(available);
    EXPECT_EQ(error, ENOSYS);
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST(IoAdapterTraits, AdaptersAreThinTriviallyCopyableViews) {
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoFile<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoFixedFile<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::TcpStream<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::TcpListener<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::UdpSocket<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoEvent<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoTimer<IoTestThread>>);
    EXPECT_LE(sizeof(af::IoFile<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoFixedFile<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::TcpStream<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::TcpListener<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::UdpSocket<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoEvent<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoTimer<IoTestThread>), 8U);
}
