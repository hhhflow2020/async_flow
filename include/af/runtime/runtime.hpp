#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/detail/thread/thread_name.hpp"
#include "af/runtime/config_resolution.hpp"
#include "af/runtime/reactor.hpp"
#include "af/runtime/task.hpp"

namespace af {

enum class runtime_state : std::uint8_t {
    stopped,
    starting,
    running,
    stopping,
};

class runtime {
public:
    using thread_index = std::uint16_t;
    using task_id_type = runtime_task_id;
    static constexpr task_id_type invalid_task_id = runtime_invalid_task_id;

    explicit runtime(runtime_config config)
        : resolution_(resolve_runtime_config(std::move(config))) {
        if (!resolution_) {
            throw std::invalid_argument(status_message(resolution_.validation));
        }
    }

    runtime(const runtime &) = delete;
    runtime &operator=(const runtime &) = delete;

    ~runtime() {
        stop();
    }

    [[nodiscard]] const runtime_config &config() const noexcept {
        return resolution_.resolved.config;
    }

    [[nodiscard]] const resolved_runtime_config &resolved_config() const noexcept {
        return resolution_.resolved;
    }

    [[nodiscard]] runtime_state state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool running() const noexcept {
        return state() == runtime_state::running;
    }

    [[nodiscard]] thread_index thread_count() const noexcept {
        return resolution_.resolved.thread_count();
    }

    [[nodiscard]] thread_index invalid_thread_index() const noexcept {
        return resolution_.resolved.invalid_thread_index();
    }

    [[nodiscard]] bool valid_thread(thread_index index) const noexcept {
        return resolution_.resolved.valid_thread(index);
    }

    [[nodiscard]] af::thread_kind thread_kind_of(thread_index index) const noexcept {
        return resolution_.resolved.thread_kind_of(index);
    }

    [[nodiscard]] std::string_view thread_name(thread_index index) const noexcept {
        return resolution_.resolved.thread_name(index);
    }

    [[nodiscard]] thread_index thread_group_offset(thread_index index) const noexcept {
        return resolution_.resolved.thread_group_offset(index);
    }

    [[nodiscard]] thread_index select_thread(thread_selector selector) const noexcept {
        return resolution_.resolved.select_thread(selector);
    }

    [[nodiscard]] thread_index active_thread_count() const noexcept {
        return active_thread_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] static runtime *current() noexcept {
        return current_runtime_;
    }

    [[nodiscard]] static thread_index current_thread_index() noexcept {
        return current_thread_index_;
    }

    [[nodiscard]] static bool is_runtime_thread() noexcept {
        return current_runtime_ != nullptr;
    }

    [[nodiscard]] static task_id_type current_task_id() noexcept {
        return current_task_id_;
    }

    [[nodiscard]] static reactor *current_reactor() noexcept;

    [[nodiscard]] bool start() {
        runtime_state expected = runtime_state::stopped;
        if (!state_.compare_exchange_strong(expected, runtime_state::starting,
                                            std::memory_order_acq_rel, std::memory_order_acquire)) {
            return false;
        }

        try {
            executors_.clear();
            executors_.reserve(resolution_.resolved.threads.size());
            for (const auto &thread : resolution_.resolved.threads) {
                executors_.push_back(std::make_unique<executor>(*this, thread));
            }
            for (auto &executor : executors_) {
                executor->start();
            }
            state_.store(runtime_state::running, std::memory_order_release);
            return true;
        } catch (...) {
            request_stop();
            join_all();
            executors_.clear();
            active_thread_count_.store(0, std::memory_order_release);
            state_.store(runtime_state::stopped, std::memory_order_release);
            throw;
        }
    }

    [[nodiscard]] bool post(thread_index thread, runtime_work *work) noexcept {
        if (work == nullptr || !valid_thread(thread)) {
            return false;
        }
        if (!try_enter_post()) {
            return false;
        }

        executors_[thread]->enqueue(work);
        leave_post();
        return true;
    }

    void stop() noexcept {
        const bool called_from_runtime_thread = current_runtime_ == this;
        runtime_state observed = state_.load(std::memory_order_acquire);
        for (;;) {
            if (observed == runtime_state::stopped) {
                return;
            }
            if (observed == runtime_state::stopping) {
                break;
            }
            if (state_.compare_exchange_weak(observed, runtime_state::stopping,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
                break;
            }
        }

        wait_for_posts();
        request_stop();
        if (called_from_runtime_thread) {
            return;
        }
        join_all();
        executors_.clear();
        active_thread_count_.store(0, std::memory_order_release);
        state_.store(runtime_state::stopped, std::memory_order_release);
    }

private:
    class executor {
    public:
        executor(runtime &owner, runtime_thread_info thread)
            : owner_(owner), thread_(std::move(thread)),
              timer_drain_budget_(owner_.config().scheduler.timer_drain_budget) {
            timers_.reserve(owner_.config().timer.initial_reserve);
            if (thread_.kind == thread_kind::io) {
                reactor_ = make_reactor(owner_.config().reactor.backend);
            }
        }

        executor(const executor &) = delete;
        executor &operator=(const executor &) = delete;

        ~executor() {
            request_stop();
            join();
        }

        void start() {
            worker_ = std::thread([this] { run_loop(); });
        }

        void request_stop() noexcept {
            stop_requested_.store(true, std::memory_order_release);
            wake_epoch_.fetch_add(1, std::memory_order_release);
            if (reactor_ != nullptr) {
                reactor_->wake();
            }
            detail::atomic_notify_all(wake_epoch_);
        }

        void enqueue(runtime_work *work) noexcept {
            inbox_.push(work);
            wake_epoch_.fetch_add(1, std::memory_order_release);
            if (reactor_ != nullptr) {
                reactor_->wake();
            }
            detail::atomic_notify_all(wake_epoch_);
        }

        void arm_timer(runtime_task *task) noexcept {
            if (!detail::runtime_task_access::mark_timer_pending(task)) {
                return;
            }
            try {
                timers_.push_back(TimerEntry{detail::runtime_task_access::timer_deadline_ns(task),
                                             next_timer_sequence_++, task});
                std::push_heap(timers_.begin(), timers_.end(), timer_entry_after);
            } catch (...) {
                detail::runtime_task_access::cancel_timer(task);
            }
        }

        void join() noexcept {
            if (!worker_.joinable()) {
                return;
            }
            if (worker_.get_id() == std::this_thread::get_id()) {
                return;
            }
            worker_.join();
        }

        [[nodiscard]] reactor *reactor_backend() noexcept {
            return reactor_.get();
        }

    private:
        void run_loop() noexcept {
            current_runtime_ = &owner_;
            current_executor_ = this;
            current_thread_index_ = thread_.index;
            if (thread_.set_os_thread_name) {
                detail::set_current_thread_name(thread_.name, thread_.group_offset);
            }

            owner_.on_executor_started();
            for (;;) {
                bool did_work = drain_inbox();
                did_work = run_due_timers() || did_work;
                if (stop_requested_.load(std::memory_order_acquire) && !did_work) {
                    break;
                }
                const auto observed = wake_epoch_.load(std::memory_order_acquire);
                if (stop_requested_.load(std::memory_order_acquire)) {
                    continue;
                }
                if (!inbox_.empty()) {
                    continue;
                }
                if (run_due_timers()) {
                    continue;
                }
                wait_for_wake_or_timer(observed);
            }
            cancel_timers();
            owner_.on_executor_stopped();
            current_thread_index_ = runtime_invalid_thread_index;
            current_executor_ = nullptr;
            current_runtime_ = nullptr;
        }

        [[nodiscard]] bool drain_inbox() noexcept {
            bool did_work = false;
            while (runtime_work *work = inbox_.try_pop()) {
                did_work = true;
                work->run(owner_);
            }
            return did_work;
        }

        struct TimerEntry {
            std::int64_t deadline_ns{0};
            std::uint64_t sequence{0};
            runtime_task *task{nullptr};
        };

        [[nodiscard]] static bool timer_entry_after(const TimerEntry &left,
                                                    const TimerEntry &right) noexcept {
            if (left.deadline_ns != right.deadline_ns) {
                return left.deadline_ns > right.deadline_ns;
            }
            return left.sequence > right.sequence;
        }

        [[nodiscard]] static std::int64_t steady_now_ns() noexcept {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }

        [[nodiscard]] std::chrono::nanoseconds timer_wait_duration() const noexcept {
            if (timers_.empty()) {
                return std::chrono::nanoseconds::max();
            }

            const std::int64_t now = steady_now_ns();
            const std::int64_t deadline = timers_.front().deadline_ns;
            if (deadline <= now) {
                return std::chrono::nanoseconds(0);
            }
            return std::chrono::nanoseconds(deadline - now);
        }

        void wait_for_wake_or_timer(std::uint32_t observed) noexcept {
            const auto timeout = timer_wait_duration();
            if (reactor_ != nullptr) {
                static_cast<void>(reactor_->poll(timeout));
                return;
            }
            if (timeout == std::chrono::nanoseconds(0)) {
                return;
            }
            if (timeout == std::chrono::nanoseconds::max()) {
                detail::atomic_wait_value(wake_epoch_, observed, std::memory_order_acquire);
                return;
            }
            static_cast<void>(detail::atomic_wait_value_for(wake_epoch_, observed, timeout,
                                                            std::memory_order_acquire));
        }

        [[nodiscard]] bool run_due_timers() noexcept {
            bool did_work = false;
            std::size_t drained = 0;
            while (drained < timer_drain_budget_ && !timers_.empty()) {
                const std::int64_t now = steady_now_ns();
                if (timers_.front().deadline_ns > now) {
                    break;
                }

                std::pop_heap(timers_.begin(), timers_.end(), timer_entry_after);
                TimerEntry entry = timers_.back();
                timers_.pop_back();
                ++drained;

                runtime_task *task = entry.task;
                if (task == nullptr || !detail::runtime_task_access::mark_timer_ready(task)) {
                    continue;
                }

                did_work = true;
                static_cast<runtime_work *>(task)->run(owner_);
            }
            return did_work;
        }

        void cancel_timers() noexcept {
            for (TimerEntry &entry : timers_) {
                detail::runtime_task_access::cancel_timer(entry.task);
            }
            timers_.clear();
        }

        runtime &owner_;
        runtime_thread_info thread_;
        detail::IntrusiveMpscQueue<runtime_work> inbox_;
        std::vector<TimerEntry> timers_;
        std::unique_ptr<reactor> reactor_;
        std::size_t timer_drain_budget_{256};
        std::uint64_t next_timer_sequence_{0};
        std::atomic<std::uint32_t> wake_epoch_{0};
        std::atomic<bool> stop_requested_{false};
        std::thread worker_;
    };

    [[nodiscard]] static std::string status_message(runtime_config_validation_result validation) {
        std::string result("invalid af::runtime_config: ");
        result.append(runtime_config_status_name(validation.status));
        result.append(" at index ");
        result.append(std::to_string(validation.index));
        return result;
    }

    void request_stop() noexcept {
        for (auto &executor : executors_) {
            executor->request_stop();
        }
    }

    void join_all() noexcept {
        for (auto &executor : executors_) {
            executor->join();
        }
    }

    [[nodiscard]] bool try_enter_post() noexcept {
        if (state_.load(std::memory_order_acquire) != runtime_state::running) {
            return false;
        }
        posting_count_.fetch_add(1, std::memory_order_acq_rel);
        if (state_.load(std::memory_order_acquire) == runtime_state::running) {
            return true;
        }
        leave_post();
        return false;
    }

    void leave_post() noexcept {
        const std::uint32_t previous = posting_count_.fetch_sub(1, std::memory_order_acq_rel);
        if (previous == 1) {
            detail::atomic_notify_all(posting_count_);
        }
    }

    void wait_for_posts() noexcept {
        for (;;) {
            const std::uint32_t observed = posting_count_.load(std::memory_order_acquire);
            if (observed == 0) {
                return;
            }
            detail::atomic_wait_value(posting_count_, observed, std::memory_order_acquire);
        }
    }

    void on_executor_started() noexcept {
        active_thread_count_.fetch_add(1, std::memory_order_acq_rel);
        active_epoch_.fetch_add(1, std::memory_order_release);
        detail::atomic_notify_all(active_epoch_);
    }

    void on_executor_stopped() noexcept {
        active_thread_count_.fetch_sub(1, std::memory_order_acq_rel);
        active_epoch_.fetch_add(1, std::memory_order_release);
        detail::atomic_notify_all(active_epoch_);
    }

    void arm_timer_on_current_executor(runtime_task *task) noexcept {
        if (current_runtime_ != this || current_executor_ == nullptr) {
            detail::runtime_task_access::cancel_timer(task);
            return;
        }
        current_executor_->arm_timer(task);
    }

    [[nodiscard]] static task_id_type exchange_current_task_id(task_id_type next) noexcept {
        const task_id_type previous = current_task_id_;
        current_task_id_ = next;
        return previous;
    }

    runtime_config_resolution resolution_;
    std::vector<std::unique_ptr<executor>> executors_;
    std::atomic<runtime_state> state_{runtime_state::stopped};
    std::atomic<thread_index> active_thread_count_{0};
    std::atomic<std::uint32_t> posting_count_{0};
    std::atomic<std::uint32_t> active_epoch_{0};

    inline static thread_local runtime *current_runtime_{nullptr};
    inline static thread_local executor *current_executor_{nullptr};
    inline static thread_local thread_index current_thread_index_{runtime_invalid_thread_index};
    inline static thread_local task_id_type current_task_id_{invalid_task_id};

    friend class runtime_task;
};

inline reactor *runtime::current_reactor() noexcept {
    if (current_executor_ == nullptr) {
        return nullptr;
    }
    return current_executor_->reactor_backend();
}

} // namespace af

#include "af/runtime/task_impl.hpp"
