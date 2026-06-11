#pragma once

#include <chrono>
#include <cstdint>

namespace af {

class runtime_task;

namespace detail {

struct runtime_timer_entry {
    std::int64_t deadline_ns{0};
    std::uint64_t sequence{0};
    runtime_task *task{nullptr};
};

[[nodiscard]] inline bool runtime_timer_entry_after(const runtime_timer_entry &left,
                                                    const runtime_timer_entry &right) noexcept {
    if (left.deadline_ns != right.deadline_ns) {
        return left.deadline_ns > right.deadline_ns;
    }
    return left.sequence > right.sequence;
}

[[nodiscard]] inline bool runtime_timer_entry_before(const runtime_timer_entry &left,
                                                     const runtime_timer_entry &right) noexcept {
    if (left.deadline_ns != right.deadline_ns) {
        return left.deadline_ns < right.deadline_ns;
    }
    return left.sequence < right.sequence;
}

[[nodiscard]] inline std::int64_t runtime_steady_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

} // namespace detail
} // namespace af
