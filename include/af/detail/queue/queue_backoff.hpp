#pragma once

#include <cstddef>
#include <thread>

#include "af/detail/runtime/cpu_relax.hpp"

namespace af::detail {

inline void queue_full_cpu_relax() noexcept {
    cpu_relax();
}

class queue_full_backoff {
public:
    explicit queue_full_backoff(std::size_t spin_count) noexcept : spin_count_(spin_count) {}

    void wait() noexcept {
        if (spins_ < spin_count_) {
            ++spins_;
            queue_full_cpu_relax();
            return;
        }

        std::this_thread::yield();
    }

    void reset() noexcept {
        spins_ = 0;
    }

private:
    const std::size_t spin_count_;
    std::size_t spins_{0};
};

using QueueFullBackoff = queue_full_backoff;

} // namespace af::detail
