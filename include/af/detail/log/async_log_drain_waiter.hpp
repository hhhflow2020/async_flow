#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>

namespace af::detail {

class async_log_drain_waiter {
public:
    async_log_drain_waiter() = default;
    async_log_drain_waiter(const async_log_drain_waiter &) = delete;
    async_log_drain_waiter &operator=(const async_log_drain_waiter &) = delete;

    template <typename PendingCounter, typename WakeFn>
    [[nodiscard]] bool
    wait_until_drained(PendingCounter &pending, std::chrono::steady_clock::time_point deadline,
                       std::chrono::milliseconds retry_interval, WakeFn &&wake_consumer) noexcept {
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
