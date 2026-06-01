#if !defined(AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_scheduler_stress_wait_fragment.hpp is a runtime scheduler stress support fragment"
#endif

inline bool wait_zero_until(
    std::atomic<int>& remaining,
    std::chrono::steady_clock::time_point deadline) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}
