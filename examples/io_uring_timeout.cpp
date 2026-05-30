#include <chrono>
#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

namespace {

enum class TimeoutThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct TimeoutRuntimeTraits {
    using Thread = TimeoutThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TimeoutThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(TimeoutThread thread) noexcept {
        return thread == TimeoutThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using timeout_async = af::AsyncRuntime<TimeoutRuntimeTraits>;
using TimeoutTaskBase = timeout_async::Task;

class RingTimeoutTask final : public TimeoutTaskBase {
public:
    explicit RingTimeoutTask(TimeoutTaskBase::FactoryToken token) : TimeoutTaskBase(token) {}

    bool do_it(int* error) {
        error_ = error;
        return schedule(TimeoutThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_wait_timeout(
            *this,
            TimeoutThread::IO_0,
            std::chrono::milliseconds(2),
            wait_);
        if (status.pending()) {
            return pending();
        }
        *error_ = status.failed() ? status.error : 0;
        return done();
    }

    af::IoOpState wait_{};
    int* error_{nullptr};
};

} // namespace

int main() {
#if defined(__linux__)
    timeout_async::init();
    if (!timeout_async::io_uring_backend_available(TimeoutThread::IO_0)) {
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
#else
    std::cout << "io_uring timeout example is Linux-only\n";
    return 0;
#endif
}
