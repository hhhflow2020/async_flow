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

bool wait_until(std::atomic<int> &value, int expected) {
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

    bool do_it(int fd, std::atomic<int> *armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(AppThreads::IO_0);
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
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult consume() {
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        std::cout << "IO_0 received byte: " << value_ << '\n';
        return done();
    }

    State state_{State::ArmRead};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    async::init();
    if (!async::io_backend_available(AppThreads::IO_0)) {
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
    const bool started = async::start_task<ReadOneByteTask>(fds[0], &armed);
    AF_ASSERT(started);

    if (!started || !wait_until(armed, 1)) {
        std::cout << "read task did not arm\n";
        ::close(fds[0]);
        ::close(fds[1]);
        async::shutdown();
        return 1;
    }

    const char value = 'A';
    static_cast<void>(::write(fds[1], &value, sizeof(value)));

    async::shutdown();
    ::close(fds[0]);
    ::close(fds[1]);
    return 0;
#else
    std::cout << "epoll IO example is Linux-only\n";
    return 0;
#endif
}
