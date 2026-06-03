#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"
#include "support/io_native_readiness_socket_pair.hpp"

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

} // namespace

int main() {
    if constexpr (!af::platform_posix) {
        std::cout << "native readiness IO example is unavailable on Windows\n";
        return 0;
    }

    using namespace io_native_readiness_example;

    NativeIoRuntime::init();
    if (!NativeIoRuntime::io_backend_available(NativeIoThreads::IO_0)) {
        std::cout << "native IO backend unavailable\n";
        NativeIoRuntime::shutdown();
        return 0;
    }

    NativeReadinessSocketPair sockets{};
    if (!sockets.create()) {
        std::cerr << "socketpair failed\n";
        NativeIoRuntime::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    const bool started = NativeIoRuntime::start_task<ReadOneByteTask>(sockets.reader.get(), &armed);
    if (!started || !wait_until(armed, 1)) {
        std::cerr << "read task did not arm\n";
        NativeIoRuntime::shutdown();
        return 1;
    }

    static_cast<void>(write_native_readiness_byte(sockets.writer.get(), 'N'));

    NativeIoRuntime::shutdown();
    return 0;
}
