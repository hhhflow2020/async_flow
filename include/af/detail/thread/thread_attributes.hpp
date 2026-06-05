#pragma once

#include <cerrno>
#include <cstdint>

#include "af/detail/config.hpp"
#include "af/runtime/config_types.hpp"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace af::detail {

[[nodiscard]] inline bool
set_current_thread_affinity(const thread_affinity_config &affinity) noexcept {
    if (affinity.cpu_ids.empty()) {
        return true;
    }

#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    for (const std::uint32_t cpu_id : affinity.cpu_ids) {
        if (cpu_id >= CPU_SETSIZE) {
            return false;
        }
        CPU_SET(static_cast<int>(cpu_id), &set);
    }
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
#else
    return false;
#endif
}

[[nodiscard]] inline bool
set_current_thread_priority(const thread_priority_config &priority) noexcept {
    if (!priority.enabled) {
        return true;
    }

#if defined(__linux__)
    const auto tid = static_cast<id_t>(::syscall(SYS_gettid));
    errno = 0;
    return ::setpriority(PRIO_PROCESS, tid, priority.value) == 0;
#else
    return false;
#endif
}

} // namespace af::detail
