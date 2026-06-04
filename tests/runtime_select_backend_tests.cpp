#include <atomic>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

#include "af/async_runtime.hpp"

#include <gtest/gtest.h>

namespace {

struct SelectIoThreadTag;

struct SelectBackendRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<SelectIoThreadTag, 1, af::thread_kind::io>("select-io"));
};

using SelectBackendRuntime = af::AsyncRuntime<SelectBackendRuntimeTraits>;
using SelectBackendTask = SelectBackendRuntime::Task;

struct SelectBackendThreads {
    static constexpr auto IO_0 =
        SelectBackendRuntime::thread_group<SelectIoThreadTag>().template at<0>();
};

class SelectBackendRuntimeGuard {
public:
    SelectBackendRuntimeGuard() {
        SelectBackendRuntime::init();
    }

    SelectBackendRuntimeGuard(const SelectBackendRuntimeGuard &) = delete;
    SelectBackendRuntimeGuard &operator=(const SelectBackendRuntimeGuard &) = delete;

    ~SelectBackendRuntimeGuard() {
        SelectBackendRuntime::shutdown();
    }
};

void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

void set_fd_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(fd, F_SETFL, flags | O_NONBLOCK), 0);
}

[[nodiscard]] bool wait_until_at_least(const std::atomic<int> &value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class SelectReadinessTask final : public SelectBackendTask {
public:
    explicit SelectReadinessTask(SelectBackendTask::FactoryToken token)
        : SelectBackendTask(token) {}

    [[nodiscard]] bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *completed,
                             std::atomic<int> *events, std::atomic<int> *error) noexcept {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        events_ = events;
        error_ = error;
        return schedule(SelectBackendThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        if (stage_ == 0) {
            stage_ = 1;
            armed_->fetch_add(1, std::memory_order_release);
            if (!wait_io(SelectBackendThreads::IO_0, fd_, af::io_readable, &result_)) {
                error_->store(result_.error, std::memory_order_release);
                completed_->fetch_add(1, std::memory_order_release);
                return failed();
            }
            return pending();
        }

        events_->store(static_cast<int>(result_.events), std::memory_order_release);
        error_->store(result_.error, std::memory_order_release);
        char byte = 0;
        const ssize_t n = ::read(fd_, &byte, sizeof(byte));
        if (n == 1 && byte == 'x' && result_.readable() && !result_.failed()) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        if (error_->load(std::memory_order_acquire) == 0) {
            error_->store(EIO, std::memory_order_release);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return failed();
    }

    int fd_{-1};
    int stage_{0};
    af::IoResult result_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *events_{nullptr};
    std::atomic<int> *error_{nullptr};
};

} // namespace

TEST(SelectBackendTests, ReadinessWaitResumesTask) {
    static_assert(af::detail::supports_select);
    static_assert(!af::detail::supports_epoll);
    static_assert(!af::detail::supports_kqueue);

    int pipe_fds[2]{-1, -1};
    ASSERT_EQ(::pipe(pipe_fds), 0);
    int read_fd = pipe_fds[0];
    int write_fd = pipe_fds[1];
    set_fd_nonblocking(read_fd);
    set_fd_nonblocking(write_fd);

    SelectBackendRuntimeGuard runtime_guard;
    ASSERT_TRUE(SelectBackendRuntime::io_backend_available(SelectBackendThreads::IO_0));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> events{0};
    std::atomic<int> error{0};

    ASSERT_TRUE(SelectBackendRuntime::start_task<SelectReadinessTask>(read_fd, &armed, &completed,
                                                                      &events, &error));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char byte = 'x';
    ASSERT_EQ(::write(write_fd, &byte, sizeof(byte)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_NE(static_cast<std::uint32_t>(events.load(std::memory_order_acquire)) & af::io_readable,
              0U);

    close_fd(read_fd);
    close_fd(write_fd);
}
