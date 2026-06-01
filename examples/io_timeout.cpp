#include <chrono>
#include <cerrno>
#include <iostream>

#include "app_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>

namespace {

void close_pair(int (&fds)[2]) {
    for (int &fd : fds) {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }
}

class ReadWithTimeoutTask final : public Task {
public:
    explicit ReadWithTimeoutTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::chrono::nanoseconds timeout, int *error) {
        fd_ = fd;
        timeout_ = timeout;
        error_ = error;
        return schedule(AppThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Resume,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Resume:
            return resume_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Resume;
        deadline_.set_after(timeout_);
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }

        const af::IoStatus timeout = af::arm_io_timeout(*this, AppThreads::IO_0, deadline_, read_);
        if (!timeout.pending()) {
            return failed();
        }
        return pending();
    }

    af::TaskResult resume_read() {
        const af::IoStatus timeout = af::arm_io_timeout(*this, AppThreads::IO_0, deadline_, read_);
        if (timeout.pending()) {
            return pending();
        }
        if (timeout.failed()) {
            *error_ = timeout.error;
            return done();
        }
        if (!timeout.ready()) {
            return failed();
        }

        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        *error_ = status.failed() ? status.error : 0;
        return done();
    }

    State state_{State::Arm};
    int fd_{-1};
    std::chrono::nanoseconds timeout_{0};
    char value_{0};
    af::IoOpState read_{};
    af::IoDeadline deadline_{};
    int *error_{nullptr};
};

} // namespace
#endif

int main() {
#if defined(__linux__)
    async::init();
    if (!async::io_backend_available(AppThreads::IO_0)) {
        std::cout << "IO backend unavailable\n";
        async::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cerr << "socketpair failed\n";
        async::shutdown();
        return 1;
    }

    int error = 0;
    if (!async::start_task<ReadWithTimeoutTask>(fds[0], std::chrono::milliseconds(5), &error)) {
        std::cerr << "timeout task did not start\n";
        close_pair(fds);
        async::shutdown();
        return 1;
    }

    async::shutdown();
    close_pair(fds);
    std::cout << "read timeout error=" << error << '\n';
    return error == ETIMEDOUT ? 0 : 1;
#else
    std::cout << "IO timeout example is Linux-only\n";
    return 0;
#endif
}
