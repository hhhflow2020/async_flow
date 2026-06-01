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

class EventTask final : public Task {
public:
    explicit EventTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::uint64_t *value) {
        event_.reset(AppThread::IO_0, fd);
        armed_ = armed;
        value_ = value;
        return schedule(AppThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return wait_event();
    }

    af::TaskResult wait_event() {
        std::uint64_t counter = 0;
        const af::IoStatus status = event_.wait(*this, &counter, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(counter) || counter == 0U) {
            return failed();
        }
        *value_ = counter;
        return done();
    }

    af::IoEvent<AppThread> event_{};
    af::IoOpState wait_{};
    std::atomic<int> *armed_{nullptr};
    std::uint64_t *value_{nullptr};
};

} // namespace
#endif

int main() {
#if defined(__linux__)
    async::init();

    if (!async::io_backend_available(AppThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        async::shutdown();
        return 0;
    }

    af::UniqueFd event = af::make_eventfd();
    if (!event) {
        std::cerr << "eventfd failed\n";
        async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::uint64_t value{0};
    if (!async::start_task<EventTask>(event.get(), &armed, &value) || !wait_until(armed, 1)) {
        std::cerr << "event task did not arm\n";
        async::shutdown();
        return 1;
    }

    int error = 0;
    if (!af::write_eventfd(event.get(), 7, error)) {
        std::cerr << "eventfd write failed: " << error << '\n';
        async::shutdown();
        return 1;
    }

    async::shutdown();
    std::cout << "event value=" << value << '\n';
    return 0;
#else
    std::cout << "eventfd example is Linux-only\n";
    return 0;
#endif
}
