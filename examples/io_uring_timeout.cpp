#include <chrono>
#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

namespace {

struct TimeoutIoThreadTag;

struct TimeoutRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<TimeoutIoThreadTag, 1, af::preferred_io_thread_kind, "timeout-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using timeout_async = af::AsyncRuntime<TimeoutRuntimeTraits>;
using TimeoutTaskBase = timeout_async::Task;
using TimeoutThread = timeout_async::Thread;

struct TimeoutThreads {
    static constexpr TimeoutThread IO_0 =
        timeout_async::thread_group<TimeoutIoThreadTag>().template at<0>();
};

class RingTimeoutTask final : public TimeoutTaskBase {
public:
    explicit RingTimeoutTask(TimeoutTaskBase::FactoryToken token) : TimeoutTaskBase(token) {}

    bool do_it(int *error) {
        error_ = error;
        return schedule(TimeoutThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status =
            af::io_wait_timeout(*this, TimeoutThreads::IO_0, std::chrono::milliseconds(2), wait_);
        if (status.pending()) {
            return pending();
        }
        *error_ = status.failed() ? status.error : 0;
        return done();
    }

    af::IoOpState wait_{};
    int *error_{nullptr};
};

} // namespace

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring timeout example is Linux-only\n";
        return 0;
    }

    timeout_async::init();
    if (!timeout_async::io_uring_backend_available(TimeoutThreads::IO_0)) {
        std::cout << "io_uring timeout backend unavailable\n";
        timeout_async::shutdown();
        return 0;
    }

    int error = 0;
    if (!timeout_async::start_task<RingTimeoutTask>(&error)) {
        std::cout << "io_uring timeout task did not start\n";
        timeout_async::shutdown();
        return 1;
    }

    timeout_async::shutdown();
    if (error == 0) {
        std::cout << "io_uring timeout fired\n";
        return 0;
    }
    std::cout << "io_uring timeout unsupported error=" << error << '\n';
    return 0;
}
