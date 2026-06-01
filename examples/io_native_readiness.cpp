#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

struct NativeIoThreadTag;

struct NativeIoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<NativeIoThreadTag, 1, af::ThreadKind::Io, "native-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
};

using NativeIoRuntime = af::AsyncRuntime<NativeIoRuntimeTraits>;
using NativeIoTask = NativeIoRuntime::Task;
using NativeIoThread = NativeIoRuntime::Thread;

struct NativeIoThreads {
    static constexpr NativeIoThread IO_0 =
        NativeIoRuntime::thread_group<NativeIoThreadTag>().template at<0>();
};

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

#if !defined(_WIN32)
bool set_nonblocking_cloexec(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return false;
    }
    const int fd_flags = ::fcntl(fd, F_GETFD, 0);
    return fd_flags >= 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
}

bool make_socket_pair(int fds[2]) {
    fds[0] = -1;
    fds[1] = -1;
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return false;
    }
    if (set_nonblocking_cloexec(fds[0]) && set_nonblocking_cloexec(fds[1])) {
        return true;
    }
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
    return false;
}

class ReadOneByteTask final : public NativeIoTask {
public:
    explicit ReadOneByteTask(NativeIoTask::FactoryToken token) : NativeIoTask(token) {}

    bool do_it(int fd, std::atomic<int> *armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(NativeIoThreads::IO_0);
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
            af::io_read_some(*this, NativeIoThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult consume() {
        const af::IoStatus status =
            af::io_read_some(*this, NativeIoThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        std::cout << "native IO received byte: " << value_ << '\n';
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
#if !defined(_WIN32)
    NativeIoRuntime::init();
    if (!NativeIoRuntime::io_backend_available(NativeIoThreads::IO_0)) {
        std::cout << "native IO backend unavailable\n";
        NativeIoRuntime::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (!make_socket_pair(fds)) {
        std::cerr << "socketpair failed\n";
        NativeIoRuntime::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    const bool started = NativeIoRuntime::start_task<ReadOneByteTask>(fds[0], &armed);
    if (!started || !wait_until(armed, 1)) {
        std::cerr << "read task did not arm\n";
        ::close(fds[0]);
        ::close(fds[1]);
        NativeIoRuntime::shutdown();
        return 1;
    }

    const char value = 'N';
    static_cast<void>(::write(fds[1], &value, sizeof(value)));

    NativeIoRuntime::shutdown();
    ::close(fds[0]);
    ::close(fds[1]);
    return 0;
#else
    std::cout << "native readiness IO example is unavailable on Windows\n";
    return 0;
#endif
}
