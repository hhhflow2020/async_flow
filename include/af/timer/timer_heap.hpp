#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <vector>

#include "af/runtime/task.hpp"
#include "af/runtime/work.hpp"
#include "af/timer/timer_entry.hpp"

namespace af::detail {

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

} // namespace af::detail
