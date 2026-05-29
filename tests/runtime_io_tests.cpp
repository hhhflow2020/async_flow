#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

template <typename T>
bool wait_until_at_least(std::atomic<T>& value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

enum class IoTestThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct IoTestRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using IoRuntime = af::AsyncRuntime<IoTestRuntimeTraits>;
using IoTaskBase = IoRuntime::Task;

struct FastIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using FastIoRuntime = af::AsyncRuntime<FastIoRuntimeTraits>;
using FastIoTaskBase = FastIoRuntime::Task;

class IoRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        IoRuntime::init();
    }

    void TearDown() override {
        IoRuntime::shutdown();
    }
};

class IoHopTask final : public IoTaskBase {
public:
    explicit IoHopTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(IoTestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Logic,
        Io,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Logic:
            state_ = State::Io;
            return pending_on(IoTestThread::IO_0);

        case State::Io:
            ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_{State::Logic};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

#if defined(__linux__)
class SocketReadableTask final : public IoTaskBase {
public:
    explicit SocketReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            state_ = State::Read;
            if (!wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result_)) {
                return failed();
            }
            armed_->fetch_add(1, std::memory_order_release);
            return pending();

        case State::Read: {
            if (!result_.readable()) {
                return failed();
            }

            char value = 0;
            const auto n = ::read(fd_, &value, sizeof(value));
            if (n != 1) {
                return failed();
            }
            byte_read_->store(value, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        }
        return failed();
    }

    State state_{State::Arm};
    int fd_{-1};
    af::IoResult result_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class PendingSocketWaitTask final : public FastIoTaskBase {
public:
    explicit PendingSocketWaitTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        if (!wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result_)) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    int fd_{-1};
    af::IoResult result_{};
    std::atomic<int>* armed_{nullptr};
};

class FastIoDoneTask final : public FastIoTaskBase {
public:
    explicit FastIoDoneTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

void close_pair(int fds[2]) {
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
}
#endif

} // namespace

TEST_F(IoRuntimeFixture, IoThreadUsesConfiguredThreadKindAndAcceptsTasks) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};

    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesTaskWhenFdBecomesReadable) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};

    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'x';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

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
