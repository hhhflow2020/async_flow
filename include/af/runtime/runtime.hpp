#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/runtime/atomic_wait.hpp"
#include "af/detail/runtime/cpu_relax.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/detail/thread/thread_attributes.hpp"
#include "af/detail/thread/thread_name.hpp"
#include "af/runtime/config_resolution.hpp"
#include "af/runtime/reactor.hpp"
#include "af/runtime/task.hpp"

namespace af {

class AsyncLogHandle;

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

    ~runtime();

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

    [[nodiscard]] bool logger_started() const noexcept {
        return owned_logger_ != nullptr;
    }

    [[nodiscard]] bool
    flush_logger(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept;

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
            start_owned_logger_if_configured();
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

    [[nodiscard]] bool register_service_task(thread_index thread,
                                             detail::RuntimeServiceTask *service) noexcept {
        if (service == nullptr || thread >= executors_.size()) {
            return false;
        }
        return executors_[thread]->register_service_task(service);
    }

    [[nodiscard]] bool unregister_service_task(thread_index thread,
                                               detail::RuntimeServiceTask *service) noexcept {
        if (service == nullptr || thread >= executors_.size()) {
            return false;
        }
        return executors_[thread]->unregister_service_task(service);
    }

    [[nodiscard]] bool wake_service_tasks(thread_index thread) noexcept {
        if (thread >= executors_.size()) {
            return false;
        }
        executors_[thread]->notify();
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
            if (observed == runtime_state::running) {
                stop_owned_logger();
            }
            if (state_.compare_exchange_weak(observed, runtime_state::stopping,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
                break;
            }
        }

        wait_for_posts();
        stop_owned_logger();
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
              task_drain_budget_(owner_.config().scheduler.task_drain_budget),
              max_task_run_slice_(owner_.config().scheduler.max_task_run_slice),
              timer_drain_budget_(owner_.config().timer.drain_budget),
              service_task_budget_(owner_.config().scheduler.service_task_budget),
              idle_wait_(owner_.config().scheduler.idle_wait),
              wake_policy_(owner_.config().scheduler.wake) {
            timers_.reserve(owner_.config().timer.initial_reserve);
            if (thread_.kind == thread_kind::io) {
                reactor_ = make_reactor(owner_.config().reactor);
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
            notify();
        }

        void notify() noexcept {
            wake_epoch_.fetch_add(1, std::memory_order_release);
            if (reactor_ != nullptr) {
                reactor_->wake();
            }
            detail::atomic_notify_all(wake_epoch_);
        }

        void enqueue(runtime_work *work) noexcept {
            inbox_.push(work);
            if (wake_policy_ == wake_policy::empty_to_non_empty) {
                if (!sleep_requested_.exchange(false, std::memory_order_acq_rel)) {
                    return;
                }
            }
            notify();
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

        [[nodiscard]] bool register_service_task(detail::RuntimeServiceTask *service) noexcept {
            AF_ASSERT(current_runtime_ == &owner_ && current_thread_index_ == thread_.index &&
                      "service task registration must run on the owner runtime thread");
            if (current_runtime_ != &owner_ || current_thread_index_ != thread_.index ||
                service == nullptr) {
                return false;
            }
            if (std::find(service_tasks_.begin(), service_tasks_.end(), service) !=
                service_tasks_.end()) {
                return true;
            }
            try {
                service_tasks_.push_back(service);
                return true;
            } catch (...) {
                return false;
            }
        }

        [[nodiscard]] bool unregister_service_task(detail::RuntimeServiceTask *service) noexcept {
            AF_ASSERT(current_runtime_ == &owner_ && current_thread_index_ == thread_.index &&
                      "service task unregister must run on the owner runtime thread");
            if (current_runtime_ != &owner_ || current_thread_index_ != thread_.index ||
                service == nullptr) {
                return false;
            }
            auto it = std::find(service_tasks_.begin(), service_tasks_.end(), service);
            if (it == service_tasks_.end()) {
                return false;
            }
            const std::size_t removed_index = static_cast<std::size_t>(it - service_tasks_.begin());
            service_tasks_.erase(it);
            if (next_service_task_ > service_tasks_.size()) {
                next_service_task_ = 0;
            } else if (next_service_task_ != 0U && next_service_task_ > removed_index) {
                --next_service_task_;
            }
            return true;
        }

    private:
        void run_loop() noexcept {
            current_runtime_ = &owner_;
            current_executor_ = this;
            current_thread_index_ = thread_.index;
            static_cast<void>(detail::set_current_thread_affinity(thread_.affinity));
            if (owner_.config().diagnostics.enable_thread_name && thread_.set_os_thread_name) {
                detail::set_current_thread_name(thread_.name, thread_.group_offset);
            }

            owner_.on_executor_started();
            for (;;) {
                bool did_work = drain_inbox();
                did_work = run_due_timers() || did_work;
                did_work = run_service_tasks() || did_work;
                if (stop_requested_.load(std::memory_order_acquire) && !did_work) {
                    break;
                }
                if (stop_requested_.load(std::memory_order_acquire)) {
                    continue;
                }
                std::uint32_t observed = 0;
                if (!prepare_wait(observed)) {
                    continue;
                }
                wait_for_wake_or_timer(observed);
                sleep_requested_.store(false, std::memory_order_release);
            }
            cancel_timers();
            owner_.on_executor_stopped();
            current_thread_index_ = runtime_invalid_thread_index;
            current_executor_ = nullptr;
            current_runtime_ = nullptr;
        }

        [[nodiscard]] bool drain_inbox() noexcept {
            if (max_task_run_slice_.count() > 0) [[unlikely]] {
                return drain_inbox_with_time_slice();
            }
            return drain_inbox_by_budget();
        }

        [[nodiscard]] bool drain_inbox_by_budget() noexcept {
            bool did_work = false;
            std::size_t drained = 0;
            while (drained < task_drain_budget_) {
                runtime_work *work = inbox_.try_pop();
                if (work == nullptr) {
                    break;
                }
                ++drained;
                did_work = true;
                work->run(owner_);
            }
            return did_work;
        }

        [[nodiscard]] bool drain_inbox_with_time_slice() noexcept {
            bool did_work = false;
            std::size_t drained = 0;
            const auto deadline = std::chrono::steady_clock::now() + max_task_run_slice_;
            while (drained < task_drain_budget_) {
                runtime_work *work = inbox_.try_pop();
                if (work == nullptr) {
                    break;
                }
                ++drained;
                did_work = true;
                work->run(owner_);
                if (std::chrono::steady_clock::now() >= deadline) {
                    break;
                }
            }
            return did_work;
        }

        [[nodiscard]] bool run_service_tasks() noexcept {
            if (service_tasks_.empty()) {
                return false;
            }
            bool did_work = false;
            const std::size_t count = service_tasks_.size();
            const std::size_t budget = service_task_budget_ < count ? service_task_budget_ : count;
            for (std::size_t i = 0; i < budget; ++i) {
                if (next_service_task_ >= service_tasks_.size()) {
                    next_service_task_ = 0;
                }
                detail::RuntimeServiceTask *service = service_tasks_[next_service_task_];
                ++next_service_task_;
                if (service == nullptr) [[unlikely]] {
                    continue;
                }
                did_work = service->run_service(service_task_budget_) || did_work;
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
            if (idle_wait_ == idle_wait_strategy::spin) {
                spin_until_wake_or_timeout(observed, timeout);
                return;
            }
            if (idle_wait_ == idle_wait_strategy::yield) {
                yield_until_wake_or_timeout(observed, timeout);
                return;
            }
            if (timeout == std::chrono::nanoseconds::max()) {
                detail::atomic_wait_value(wake_epoch_, observed, std::memory_order_acquire);
                return;
            }
            static_cast<void>(detail::atomic_wait_value_for(wake_epoch_, observed, timeout,
                                                            std::memory_order_acquire));
        }

        [[nodiscard]] bool wake_observed(std::uint32_t observed) const noexcept {
            return stop_requested_.load(std::memory_order_acquire) ||
                   wake_epoch_.load(std::memory_order_acquire) != observed;
        }

        void spin_until_wake_or_timeout(std::uint32_t observed,
                                        std::chrono::nanoseconds timeout) noexcept {
            wait_polling_until_wake_or_timeout(observed, timeout, false);
        }

        void yield_until_wake_or_timeout(std::uint32_t observed,
                                         std::chrono::nanoseconds timeout) noexcept {
            wait_polling_until_wake_or_timeout(observed, timeout, true);
        }

        void wait_polling_until_wake_or_timeout(std::uint32_t observed,
                                                std::chrono::nanoseconds timeout,
                                                bool yield_wait) noexcept {
            if (timeout == std::chrono::nanoseconds::max()) {
                while (!wake_observed(observed)) {
                    idle_wait_once(yield_wait);
                }
                return;
            }

            const std::int64_t now = steady_now_ns();
            const std::int64_t timeout_ns = timeout.count();
            const std::int64_t deadline =
                timeout_ns > std::numeric_limits<std::int64_t>::max() - now
                    ? std::numeric_limits<std::int64_t>::max()
                    : now + timeout_ns;
            while (!wake_observed(observed) && steady_now_ns() < deadline) {
                idle_wait_once(yield_wait);
            }
        }

        static void idle_wait_once(bool yield_wait) noexcept {
            if (yield_wait) {
                std::this_thread::yield();
                return;
            }
            detail::cpu_relax();
        }

        [[nodiscard]] bool prepare_wait(std::uint32_t &observed) noexcept {
            if (wake_policy_ == wake_policy::empty_to_non_empty) {
                sleep_requested_.store(true, std::memory_order_release);
            }
            observed = wake_epoch_.load(std::memory_order_acquire);
            if (stop_requested_.load(std::memory_order_acquire) || !inbox_.empty()) {
                sleep_requested_.store(false, std::memory_order_release);
                return false;
            }
            if (run_due_timers() || run_service_tasks()) {
                sleep_requested_.store(false, std::memory_order_release);
                return false;
            }
            return true;
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
        std::vector<detail::RuntimeServiceTask *> service_tasks_;
        std::unique_ptr<reactor> reactor_;
        std::size_t task_drain_budget_{256};
        std::chrono::nanoseconds max_task_run_slice_{0};
        std::size_t timer_drain_budget_{256};
        std::size_t service_task_budget_{32};
        std::size_t next_service_task_{0};
        std::uint64_t next_timer_sequence_{0};
        idle_wait_strategy idle_wait_{idle_wait_strategy::futex};
        wake_policy wake_policy_{wake_policy::empty_to_non_empty};
        alignas(detail::hardware_cache_line_size) std::atomic<bool> sleep_requested_{false};
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint32_t> wake_epoch_{0};
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

    void start_owned_logger_if_configured();
    void stop_owned_logger() noexcept;

    runtime_config_resolution resolution_;
    std::vector<std::unique_ptr<executor>> executors_;
    std::unique_ptr<AsyncLogHandle> owned_logger_;
    std::atomic<runtime_state> state_{runtime_state::stopped};
    std::atomic<bool> owned_logger_stop_started_{false};
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

namespace detail {

inline const task_pool_config &runtime_task_pool_config(const runtime &owner) noexcept {
    return owner.config().task_pool;
}

[[noreturn]] inline void handle_runtime_task_bad_alloc(const runtime &owner) {
    if (owner.config().task_pool.oom == oom_policy::fatal) {
        std::terminate();
    }
    throw std::bad_alloc();
}

} // namespace detail

} // namespace af

#include "af/detail/log/absl_log_sink.hpp"

namespace af {

inline runtime::~runtime() {
    stop();
}

inline bool runtime::flush_logger(std::chrono::milliseconds timeout) noexcept {
    return owned_logger_ == nullptr || owned_logger_->flush(timeout);
}

inline void runtime::start_owned_logger_if_configured() {
    if (owned_logger_ != nullptr || config().logger.backends.empty()) {
        return;
    }
    owned_logger_stop_started_.store(false, std::memory_order_release);
    owned_logger_ = start_async_logging_for_runtime(*this);
}

inline void runtime::stop_owned_logger() noexcept {
    bool expected = false;
    if (!owned_logger_stop_started_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    if (owned_logger_ == nullptr) {
        return;
    }
    owned_logger_->stop();
    owned_logger_.reset();
}

} // namespace af

#include "af/runtime/task_impl.hpp"
