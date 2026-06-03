#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>

namespace af::detail {

class AsyncLogDrainWaiter {
public:
    AsyncLogDrainWaiter() = default;
    AsyncLogDrainWaiter(const AsyncLogDrainWaiter &) = delete;
    AsyncLogDrainWaiter &operator=(const AsyncLogDrainWaiter &) = delete;

    template <typename WakeFn>
    [[nodiscard]] bool wait_until_drained(std::atomic<std::size_t> &pending,
                                          std::chrono::steady_clock::time_point deadline,
                                          std::chrono::milliseconds retry_interval,
                                          WakeFn &&wake_consumer) noexcept {
        if (pending.load(std::memory_order_acquire) == 0U) {
            return true;
        }
        if (retry_interval <= std::chrono::milliseconds(0)) [[unlikely]] {
            retry_interval = std::chrono::milliseconds(1);
        }

        while (pending.load(std::memory_order_acquire) != 0U) {
            wake_consumer();
            auto retry_deadline = std::chrono::steady_clock::now() + retry_interval;
            if (retry_deadline > deadline) {
                retry_deadline = deadline;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return pending.load(std::memory_order_acquire) == 0U;
            }
            std::this_thread::sleep_until(retry_deadline);
        }
        return true;
    }

    void notify_drained() noexcept {}
};

} // namespace af::detail
