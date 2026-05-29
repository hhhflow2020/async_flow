#pragma once

#include <atomic>
#include <cstdint>

#include "af/async_flow.hpp"

enum class AppThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    IO_0,
    enum_num_end,
};

struct AppRuntimeTraits {
    using Thread = AppThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AppThread::enum_num_end);
    static constexpr AppThread logic_begin = AppThread::Logic_0;
    static constexpr std::uint16_t logic_count = 4;
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using Runtime = af::AsyncRuntime<AppRuntimeTraits>;
using Task = Runtime::Task;

inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return Runtime::shard_by<AppRuntimeTraits::logic_begin, AppRuntimeTraits::logic_count>(
        player_id);
}

inline void wait_completed(std::atomic<int>& completed, int expected) {
    while (completed.load(std::memory_order_acquire) < expected) {
        const int observed = completed.load(std::memory_order_acquire);
        if (observed < expected) {
            completed.wait(observed, std::memory_order_acquire);
        }
    }
}
