#include "runtime_io_test_support.hpp"

TEST(RuntimeIo, StopImmediatelyDropsPendingIoWaitsAndCanRestart) {
#if defined(__linux__)
    FastIoRuntime::init();
    if (!FastIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        FastIoRuntime::shutdown();
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        FastIoRuntime::shutdown();
        FAIL() << "socketpair failed";
    }

    std::atomic<int> armed{0};
    if (!FastIoRuntime::start_task<PendingSocketWaitTask>(fds[0], &armed)) {
        close_pair(fds);
        FastIoRuntime::shutdown();
        FAIL() << "failed to start pending IO task";
    }
    if (!wait_until_at_least(armed, 1)) {
        close_pair(fds);
        FastIoRuntime::shutdown();
        FAIL() << "pending IO task was not armed";
    }

    FastIoRuntime::shutdown();
    close_pair(fds);

    FastIoRuntime::init();
    std::atomic<int> completed{0};
    const bool started = FastIoRuntime::start_task<FastIoDoneTask>(&completed);
    EXPECT_TRUE(started);
    if (started) {
        EXPECT_TRUE(wait_until_at_least(completed, 1));
    }
    FastIoRuntime::shutdown();
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
