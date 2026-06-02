#pragma once

#include <cstddef>
#include <thread>

namespace af::detail {

inline void queue_full_cpu_relax() noexcept {
#if (defined(__x86_64__) || defined(__i386__)) && (defined(__GNUC__) || defined(__clang__))
    __asm__ __volatile__("pause" ::: "memory");
#elif (defined(__aarch64__) || defined(__arm__)) && (defined(__GNUC__) || defined(__clang__))
    __asm__ __volatile__("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

class QueueFullBackoff {
public:
    explicit QueueFullBackoff(std::size_t spin_count) noexcept : spin_count_(spin_count) {}

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

} // namespace af::detail
