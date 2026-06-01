#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "app_runtime.hpp"

#if defined(__linux__)
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

class TimerTask final : public Task {
public:
    explicit TimerTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::uint64_t *expirations) {
        timer_.reset(AppThreads::IO_0, fd);
        armed_ = armed;
        expirations_ = expirations;
        return schedule(AppThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        return wait_timer();
    }

    af::TaskResult wait_timer() {
        std::uint64_t count = 0;
        const af::IoStatus status = timer_.wait(*this, &count, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(count) || count == 0U) {
            return failed();
        }
        *expirations_ = count;
        return done();
    }

    af::IoTimer<AppThread> timer_{};
    af::IoOpState wait_{};
    std::atomic<int> *armed_{nullptr};
    std::uint64_t *expirations_{nullptr};
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

    af::UniqueFd timer = af::make_timerfd();
    if (!timer) {
        std::cerr << "timerfd_create failed\n";
        async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::uint64_t expirations{0};
    if (!async::start_task<TimerTask>(timer.get(), &armed, &expirations) || !wait_until(armed, 1)) {
        std::cerr << "timer task did not arm\n";
        async::shutdown();
        return 1;
    }

    int error = 0;
    if (!af::arm_timerfd_after(timer.get(), std::chrono::milliseconds(5), error)) {
        std::cerr << "timerfd_settime failed: " << error << '\n';
        async::shutdown();
        return 1;
    }

    async::shutdown();
    std::cout << "timer expirations=" << expirations << '\n';
    return 0;
#else
    std::cout << "timerfd example is Linux-only\n";
    return 0;
#endif
}
