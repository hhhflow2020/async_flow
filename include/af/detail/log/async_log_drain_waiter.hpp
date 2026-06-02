#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>

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

        std::unique_lock lock(mutex_);
        while (pending.load(std::memory_order_acquire) != 0U) {
            wake_consumer();
            auto retry_deadline = std::chrono::steady_clock::now() + retry_interval;
            if (retry_deadline > deadline) {
                retry_deadline = deadline;
            }
            if (!cv_.wait_until(
                    lock, retry_deadline,
                    [&pending] { return pending.load(std::memory_order_acquire) == 0U; }) &&
                std::chrono::steady_clock::now() >= deadline &&
                pending.load(std::memory_order_acquire) != 0U) {
                return false;
            }
        }
        return true;
    }

    void notify_drained() noexcept {
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
};

} // namespace af::detail
