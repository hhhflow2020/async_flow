#pragma once

#include <atomic>
#include <chrono>
#include <thread>

namespace af::detail {

template <typename AtomicBool>
[[nodiscard]] bool wait_until_atomic_flag_true(
    const AtomicBool &flag, std::chrono::steady_clock::time_point deadline,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(1)) noexcept {
    if (poll_interval <= std::chrono::milliseconds(0)) [[unlikely]] {
        poll_interval = std::chrono::milliseconds(1);
    }

    while (!flag.load(std::memory_order_acquire)) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return flag.load(std::memory_order_acquire);
        }

        auto wake_time = now + poll_interval;
        if (wake_time > deadline) {
            wake_time = deadline;
        }
        std::this_thread::sleep_until(wake_time);
    }
    return true;
}

} // namespace af::detail
