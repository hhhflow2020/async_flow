#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "app_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

bool wait_until(std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__)
class ReadOneByteTask final : public Task {
public:
    explicit ReadOneByteTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* completed) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        return schedule(AppThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ArmRead,
        Consume,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ArmRead:
            return arm_read();

        case State::Consume:
            return consume();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Consume;
        if (!wait_io(AppThread::IO_0, fd_, af::io_readable, &result_)) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult consume() {
        if (!result_.readable()) {
            return failed();
        }

        char value = 0;
        const auto n = ::read(fd_, &value, sizeof(value));
        if (n != 1) {
            return failed();
        }
        std::cout << "IO_0 received byte: " << value << '\n';
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::ArmRead};
    int fd_{-1};
    af::IoResult result_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    async::init();
    if (!async::io_backend_available(AppThread::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        async::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cout << "socketpair failed\n";
        async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    const bool started = async::start_task<ReadOneByteTask>(fds[0], &armed, &completed);
    AF_ASSERT(started);

    if (started && wait_until(armed, 1)) {
        const char value = 'A';
        static_cast<void>(::write(fds[1], &value, sizeof(value)));
    }

    if (!wait_until(completed, 1)) {
        std::cout << "read task timed out\n";
    }

    ::close(fds[0]);
    ::close(fds[1]);
    async::shutdown();
    return 0;
#else
    std::cout << "epoll IO example is Linux-only\n";
    return 0;
#endif
}
