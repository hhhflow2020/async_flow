#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

#include "af/runtime/config_types.hpp"
#include "af/runtime/task.hpp"
#include "af/runtime/work.hpp"

namespace af {

class runtime;

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

class runtime_timer_heap {
public:
    void reserve(std::size_t capacity) {
        entries_.reserve(capacity);
    }

    void push(runtime_timer_entry entry) {
        entries_.push_back(entry);
        std::push_heap(entries_.begin(), entries_.end(), runtime_timer_entry_after);
    }

    [[nodiscard]] std::chrono::nanoseconds wait_duration(std::int64_t now_ns) const noexcept {
        if (entries_.empty()) {
            return std::chrono::nanoseconds::max();
        }
        const std::int64_t deadline = entries_.front().deadline_ns;
        if (deadline <= now_ns) {
            return std::chrono::nanoseconds(0);
        }
        return std::chrono::nanoseconds(deadline - now_ns);
    }

    template <typename Runner>
    [[nodiscard]] bool run_due(std::int64_t now_ns, std::size_t budget, Runner &&runner) noexcept {
        bool did_work = false;
        std::size_t drained = 0;
        while (drained < budget && !entries_.empty()) {
            if (entries_.front().deadline_ns > now_ns) {
                break;
            }

            std::pop_heap(entries_.begin(), entries_.end(), runtime_timer_entry_after);
            runtime_timer_entry entry = entries_.back();
            entries_.pop_back();
            ++drained;

            runtime_task *task = entry.task;
            if (task == nullptr || !runtime_task_access::mark_timer_ready(task)) {
                continue;
            }

            did_work = true;
            runner(static_cast<runtime_work *>(task));
        }
        return did_work;
    }

    void cancel_all() noexcept {
        for (runtime_timer_entry &entry : entries_) {
            runtime_task_access::cancel_timer(entry.task);
        }
        entries_.clear();
    }

private:
    std::vector<runtime_timer_entry> entries_;
};

class runtime_hierarchical_timer_wheel {
public:
    runtime_hierarchical_timer_wheel() = default;

    void configure(const timer_config &config, std::size_t drain_budget) {
        tick_ns_ = std::chrono::duration_cast<std::chrono::nanoseconds>(config.tick).count();
        if (tick_ns_ <= 0) {
            tick_ns_ = 1;
        }
        slot_count_ = config.wheel_slots == 0 ? 1 : config.wheel_slots;
        slot_mask_ = is_power_of_two(slot_count_) ? slot_count_ - 1U : 0U;
        level1_span_ticks_ = saturating_square(static_cast<std::uint64_t>(slot_count_));
        level0_.resize(slot_count_);
        level1_.resize(slot_count_);
        overflow_.reserve(config.initial_reserve);
        due_buffer_.reserve(drain_budget);
    }

    void push(runtime_timer_entry entry, std::int64_t now_ns) {
        refresh_current_tick(now_ns);
        const std::uint64_t deadline_tick = tick_for(entry.deadline_ns);
        const std::uint64_t distance =
            deadline_tick > current_tick_ ? deadline_tick - current_tick_ : 0U;
        if (distance < static_cast<std::uint64_t>(slot_count_)) {
            level0_[slot_index(deadline_tick)].push_back(entry);
        } else if (distance < level1_span_ticks_) {
            level1_[level1_slot_index(deadline_tick)].push_back(entry);
        } else {
            overflow_.push_back(entry);
            std::push_heap(overflow_.begin(), overflow_.end(), runtime_timer_entry_after);
        }
        ++pending_count_;
        if (entry.deadline_ns < next_deadline_ns_) {
            next_deadline_ns_ = entry.deadline_ns;
        }
    }

    [[nodiscard]] std::chrono::nanoseconds wait_duration(std::int64_t now_ns) const noexcept {
        if (pending_count_ == 0) {
            return std::chrono::nanoseconds::max();
        }
        if (next_deadline_ns_ <= now_ns) {
            return std::chrono::nanoseconds(0);
        }
        return std::chrono::nanoseconds(next_deadline_ns_ - now_ns);
    }

    template <typename Runner>
    [[nodiscard]] bool run_due(std::int64_t now_ns, std::size_t budget, Runner &&runner) noexcept {
        if (pending_count_ == 0 || budget == 0 || next_deadline_ns_ > now_ns) {
            return false;
        }

        refresh_current_tick(now_ns);
        due_buffer_.clear();
        const std::uint64_t next_tick = tick_for(next_deadline_ns_);
        collect_due_from_bucket(level0_[slot_index(next_tick)], now_ns, budget, due_buffer_);
        if (due_buffer_.size() < budget) {
            collect_due_from_bucket(level1_[level1_slot_index(next_tick)], now_ns, budget,
                                    due_buffer_);
        }
        if (due_buffer_.size() < budget) {
            collect_due_from_overflow(now_ns, budget, due_buffer_);
        }
        if (due_buffer_.empty() && next_deadline_ns_ <= now_ns) [[unlikely]] {
            collect_due_from_all(now_ns, budget, due_buffer_);
        }

        if (due_buffer_.empty()) [[unlikely]] {
            rebuild_next_deadline();
            return false;
        }

        if (due_buffer_.size() > 1U) {
            std::sort(due_buffer_.begin(), due_buffer_.end(), runtime_timer_entry_before);
        }
        pending_count_ -= due_buffer_.size();
        bool did_work = false;
        for (runtime_timer_entry &entry : due_buffer_) {
            runtime_task *task = entry.task;
            if (task == nullptr || !runtime_task_access::mark_timer_ready(task)) {
                continue;
            }
            did_work = true;
            runner(static_cast<runtime_work *>(task));
        }
        rebuild_next_deadline();
        return did_work;
    }

    void cancel_all() noexcept {
        for (auto &bucket : level0_) {
            cancel_bucket(bucket);
            bucket.clear();
        }
        for (auto &bucket : level1_) {
            cancel_bucket(bucket);
            bucket.clear();
        }
        cancel_bucket(overflow_);
        overflow_.clear();
        due_buffer_.clear();
        pending_count_ = 0;
        next_deadline_ns_ = std::numeric_limits<std::int64_t>::max();
    }

private:
    [[nodiscard]] static bool is_power_of_two(std::size_t value) noexcept {
        return value != 0 && (value & (value - 1U)) == 0;
    }

    [[nodiscard]] static std::uint64_t saturating_square(std::uint64_t value) noexcept {
        if (value != 0 && value > std::numeric_limits<std::uint64_t>::max() / value) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        return value * value;
    }

    [[nodiscard]] std::uint64_t tick_for(std::int64_t deadline_ns) const noexcept {
        if (deadline_ns <= 0) {
            return 0;
        }
        return static_cast<std::uint64_t>(deadline_ns) / static_cast<std::uint64_t>(tick_ns_);
    }

    [[nodiscard]] std::size_t slot_index(std::uint64_t tick) const noexcept {
        if (slot_mask_ != 0U) {
            return static_cast<std::size_t>(tick & static_cast<std::uint64_t>(slot_mask_));
        }
        return static_cast<std::size_t>(tick % static_cast<std::uint64_t>(slot_count_));
    }

    [[nodiscard]] std::size_t level1_slot_index(std::uint64_t tick) const noexcept {
        return slot_index(tick / static_cast<std::uint64_t>(slot_count_));
    }

    void refresh_current_tick(std::int64_t now_ns) noexcept {
        const std::uint64_t now_tick = tick_for(now_ns);
        if (!initialized_ || now_tick > current_tick_) {
            current_tick_ = now_tick;
            initialized_ = true;
        }
    }

    static void collect_due_from_bucket(std::vector<runtime_timer_entry> &bucket,
                                        std::int64_t now_ns, std::size_t budget,
                                        std::vector<runtime_timer_entry> &out) noexcept {
        if (bucket.empty() || out.size() >= budget) {
            return;
        }
        std::size_t write = 0;
        for (std::size_t read = 0; read < bucket.size(); ++read) {
            runtime_timer_entry &entry = bucket[read];
            if (entry.deadline_ns <= now_ns && out.size() < budget) {
                out.push_back(entry);
                continue;
            }
            if (write != read) {
                bucket[write] = entry;
            }
            ++write;
        }
        bucket.resize(write);
    }

    static void cancel_bucket(const std::vector<runtime_timer_entry> &bucket) noexcept {
        for (const runtime_timer_entry &entry : bucket) {
            runtime_task_access::cancel_timer(entry.task);
        }
    }

    void collect_due_from_overflow(std::int64_t now_ns, std::size_t budget,
                                   std::vector<runtime_timer_entry> &out) noexcept {
        while (out.size() < budget && !overflow_.empty() &&
               overflow_.front().deadline_ns <= now_ns) {
            std::pop_heap(overflow_.begin(), overflow_.end(), runtime_timer_entry_after);
            out.push_back(overflow_.back());
            overflow_.pop_back();
        }
    }

    void collect_due_from_all(std::int64_t now_ns, std::size_t budget,
                              std::vector<runtime_timer_entry> &out) noexcept {
        for (auto &bucket : level0_) {
            collect_due_from_bucket(bucket, now_ns, budget, out);
            if (out.size() >= budget) {
                return;
            }
        }
        for (auto &bucket : level1_) {
            collect_due_from_bucket(bucket, now_ns, budget, out);
            if (out.size() >= budget) {
                return;
            }
        }
        collect_due_from_overflow(now_ns, budget, out);
    }

    void rebuild_next_deadline() noexcept {
        std::int64_t next = std::numeric_limits<std::int64_t>::max();
        for (const auto &bucket : level0_) {
            for (const runtime_timer_entry &entry : bucket) {
                if (entry.deadline_ns < next) {
                    next = entry.deadline_ns;
                }
            }
        }
        for (const auto &bucket : level1_) {
            for (const runtime_timer_entry &entry : bucket) {
                if (entry.deadline_ns < next) {
                    next = entry.deadline_ns;
                }
            }
        }
        for (const runtime_timer_entry &entry : overflow_) {
            if (entry.deadline_ns < next) {
                next = entry.deadline_ns;
            }
        }
        next_deadline_ns_ = pending_count_ == 0 ? std::numeric_limits<std::int64_t>::max() : next;
    }

    std::int64_t tick_ns_{1};
    std::size_t slot_count_{1};
    std::size_t slot_mask_{0};
    std::uint64_t level1_span_ticks_{1};
    std::uint64_t current_tick_{0};
    bool initialized_{false};
    std::size_t pending_count_{0};
    std::int64_t next_deadline_ns_{std::numeric_limits<std::int64_t>::max()};
    std::vector<std::vector<runtime_timer_entry>> level0_;
    std::vector<std::vector<runtime_timer_entry>> level1_;
    std::vector<runtime_timer_entry> overflow_;
    std::vector<runtime_timer_entry> due_buffer_;
};

} // namespace detail
} // namespace af
