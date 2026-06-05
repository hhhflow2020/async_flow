#pragma once

#include <thread>

namespace af::detail {

inline void cpu_relax() noexcept {
#if (defined(__x86_64__) || defined(__i386__)) && (defined(__GNUC__) || defined(__clang__))
    __asm__ __volatile__("pause" ::: "memory");
#elif (defined(__aarch64__) || defined(__arm__)) && (defined(__GNUC__) || defined(__clang__))
    __asm__ __volatile__("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

} // namespace af::detail
