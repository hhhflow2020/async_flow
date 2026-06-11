#pragma once

#include <algorithm>
#include <limits>
#include <utility>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/detail/runtime/cpu_relax.hpp"
#include "af/detail/thread/thread_attributes.hpp"
#include "af/detail/thread/thread_name.hpp"
#include "af/runtime/detail/executor.hpp"

namespace af::detail {

inline runtime_executor::runtime_executor(runtime &owner, runtime_thread_info thread)
    : owner_(owner), thread_(std::move(thread)),
      task_drain_budget_(owner_.config().scheduler.task_drain_budget),
      max_task_run_slice_(owner_.config().scheduler.max_task_run_slice),
      timer_drain_budget_(owner_.config().timer.drain_budget),
      timer_kind_(owner_.config().timer.kind),
      service_task_budget_(owner_.config().scheduler.service_task_budget),
      idle_wait_(owner_.config().scheduler.idle_wait),
      wake_policy_(owner_.config().scheduler.wake) {
    if (timer_kind_ == timer_kind::min_heap) {
        timer_heap_.reserve(owner_.config().timer.initial_reserve);
    } else {
        timer_wheel_.configure(owner_.config().timer, timer_drain_budget_);
    }
    if (thread_.kind == thread_kind::io) {
        reactor_ = make_reactor(owner_.config().reactor);
    }
}

inline runtime_executor::~runtime_executor() {
    request_stop();
    join();
}

inline void runtime_executor::start() {
    worker_ = std::thread([this] { run_loop(); });
}

inline void runtime_executor::request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    notify();
}

inline void runtime_executor::notify() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (reactor_ != nullptr) {
        reactor_->wake();
    }
    atomic_notify_all(wake_epoch_);
}

inline void runtime_executor::enqueue(runtime_work *work) noexcept {
    const std::size_t previous_queued = queued_work_count_.fetch_add(1, std::memory_order_acq_rel);
    inbox_.push(work);
    if (runtime::current_runtime_ == &owner_ && runtime::current_thread_index_ == thread_.index) {
        return;
    }
    if (wake_policy_ == wake_policy::empty_to_non_empty && previous_queued != 0U) {
        return;
    }
    notify();
}

inline void runtime_executor::arm_timer(runtime_task *task) noexcept {
    if (!runtime_task_access::mark_timer_pending(task)) {
        return;
    }
    try {
        runtime_timer_entry entry{runtime_task_access::timer_deadline_ns(task),
                                  next_timer_sequence_++, task};
        if (timer_kind_ == timer_kind::min_heap) {
            timer_heap_.push(entry);
        } else {
            timer_wheel_.push(entry, steady_now_ns());
        }
    } catch (...) {
        runtime_task_access::cancel_timer(task);
    }
}

inline void runtime_executor::join() noexcept {
    if (!worker_.joinable()) {
        return;
    }
    if (worker_.get_id() == std::this_thread::get_id()) {
        return;
    }
    worker_.join();
}

inline reactor *runtime_executor::reactor_backend() noexcept {
    return reactor_.get();
}

inline bool runtime_executor::register_service_task(runtime_service_task *service) noexcept {
    AF_ASSERT(runtime::current_runtime_ == &owner_ &&
              runtime::current_thread_index_ == thread_.index &&
              "service task registration must run on the owner runtime thread");
    if (runtime::current_runtime_ != &owner_ || runtime::current_thread_index_ != thread_.index ||
        service == nullptr) {
        return false;
    }
    if (std::find(service_tasks_.begin(), service_tasks_.end(), service) != service_tasks_.end()) {
        return true;
    }
    try {
        service_tasks_.push_back(service);
        return true;
    } catch (...) {
        return false;
    }
}

inline bool runtime_executor::unregister_service_task(runtime_service_task *service) noexcept {
    AF_ASSERT(runtime::current_runtime_ == &owner_ &&
              runtime::current_thread_index_ == thread_.index &&
              "service task unregister must run on the owner runtime thread");
    if (runtime::current_runtime_ != &owner_ || runtime::current_thread_index_ != thread_.index ||
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

inline void runtime_executor::run_loop() noexcept {
    runtime::current_runtime_ = &owner_;
    runtime::current_executor_ = this;
    runtime::current_thread_index_ = thread_.index;
    static_cast<void>(set_current_thread_affinity(thread_.affinity));
    static_cast<void>(set_current_thread_priority(thread_.priority));
    if (owner_.config().diagnostics.enable_thread_name && thread_.set_os_thread_name) {
        set_current_thread_name(thread_.name, thread_.group_offset);
    }

    owner_.on_executor_started();
    for (;;) {
        bool did_work = drain_inbox();
        did_work = run_due_timers() || did_work;
        did_work = run_service_tasks() || did_work;
        if (stop_requested_.load(std::memory_order_acquire) && !did_work) {
            if (!owner_.has_active_work()) {
                break;
            }
            std::this_thread::yield();
            continue;
        }
        if (stop_requested_.load(std::memory_order_acquire)) {
            continue;
        }
        std::uint32_t observed = 0;
        if (!prepare_wait(observed)) {
            continue;
        }
        wait_for_wake_or_timer(observed);
    }
    cancel_timers();
    owner_.on_executor_stopped();
    runtime::current_thread_index_ = runtime_invalid_thread_index;
    runtime::current_executor_ = nullptr;
    runtime::current_runtime_ = nullptr;
}

inline bool runtime_executor::drain_inbox() noexcept {
    if (max_task_run_slice_.count() > 0) [[unlikely]] {
        return drain_inbox_with_time_slice();
    }
    return drain_inbox_by_budget();
}

inline bool runtime_executor::drain_inbox_by_budget() noexcept {
    bool did_work = false;
    std::size_t drained = 0;
    while (drained < task_drain_budget_) {
        runtime_work *work = inbox_.try_pop();
        if (work == nullptr) {
            break;
        }
        queued_work_count_.fetch_sub(1, std::memory_order_acq_rel);
        ++drained;
        did_work = true;
        run_work(work);
    }
    return did_work;
}

inline bool runtime_executor::drain_inbox_with_time_slice() noexcept {
    bool did_work = false;
    std::size_t drained = 0;
    const auto deadline = std::chrono::steady_clock::now() + max_task_run_slice_;
    while (drained < task_drain_budget_) {
        runtime_work *work = inbox_.try_pop();
        if (work == nullptr) {
            break;
        }
        queued_work_count_.fetch_sub(1, std::memory_order_acq_rel);
        ++drained;
        did_work = true;
        run_work(work);
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
    }
    return did_work;
}

inline bool runtime_executor::run_service_tasks() noexcept {
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
        runtime_service_task *service = service_tasks_[next_service_task_];
        ++next_service_task_;
        if (service == nullptr) [[unlikely]] {
            continue;
        }
        owner_.track_work_started();
        did_work = service->run_service(service_task_budget_) || did_work;
        owner_.track_work_finished();
    }
    return did_work;
}

inline void runtime_executor::run_work(runtime_work *work) noexcept {
    work->run(owner_);
    owner_.track_work_finished();
}

inline void runtime_executor::run_unqueued_work(runtime_work *work) noexcept {
    owner_.track_work_started();
    run_work(work);
}

inline std::int64_t runtime_executor::steady_now_ns() noexcept {
    return runtime_steady_now_ns();
}

inline std::chrono::nanoseconds runtime_executor::timer_wait_duration() const noexcept {
    const std::int64_t now = steady_now_ns();
    if (timer_kind_ == timer_kind::hierarchical_wheel) {
        return timer_wheel_.wait_duration(now);
    }
    return timer_heap_.wait_duration(now);
}

inline void runtime_executor::wait_for_wake_or_timer(std::uint32_t observed) noexcept {
    const auto timeout = timer_wait_duration();
    if (reactor_ != nullptr) {
        owner_.track_work_started();
        static_cast<void>(reactor_->poll(timeout));
        owner_.track_work_finished();
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
        atomic_wait_value(wake_epoch_, observed, std::memory_order_acquire);
        return;
    }
    static_cast<void>(
        atomic_wait_value_for(wake_epoch_, observed, timeout, std::memory_order_acquire));
}

inline bool runtime_executor::wake_observed(std::uint32_t observed) const noexcept {
    return stop_requested_.load(std::memory_order_acquire) ||
           wake_epoch_.load(std::memory_order_acquire) != observed;
}

inline void
runtime_executor::spin_until_wake_or_timeout(std::uint32_t observed,
                                             std::chrono::nanoseconds timeout) noexcept {
    wait_polling_until_wake_or_timeout(observed, timeout, false);
}

inline void
runtime_executor::yield_until_wake_or_timeout(std::uint32_t observed,
                                              std::chrono::nanoseconds timeout) noexcept {
    wait_polling_until_wake_or_timeout(observed, timeout, true);
}

inline void runtime_executor::wait_polling_until_wake_or_timeout(std::uint32_t observed,
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
    const std::int64_t deadline = timeout_ns > std::numeric_limits<std::int64_t>::max() - now
                                      ? std::numeric_limits<std::int64_t>::max()
                                      : now + timeout_ns;
    while (!wake_observed(observed) && steady_now_ns() < deadline) {
        idle_wait_once(yield_wait);
    }
}

inline void runtime_executor::idle_wait_once(bool yield_wait) noexcept {
    if (yield_wait) {
        std::this_thread::yield();
        return;
    }
    cpu_relax();
}

inline bool runtime_executor::prepare_wait(std::uint32_t &observed) noexcept {
    observed = wake_epoch_.load(std::memory_order_acquire);
    if (stop_requested_.load(std::memory_order_acquire) ||
        queued_work_count_.load(std::memory_order_acquire) != 0U || !inbox_.empty()) {
        return false;
    }
    if (run_due_timers() || run_service_tasks()) {
        return false;
    }
    return true;
}

inline bool runtime_executor::run_due_timers() noexcept {
    const std::int64_t now = steady_now_ns();
    const auto run = [this](runtime_work *work) noexcept { run_unqueued_work(work); };
    auto run_due = [this](auto &timer_backend, std::int64_t now_ns, const auto &runner) noexcept {
        if (timer_backend.wait_duration(now_ns) != std::chrono::nanoseconds(0)) {
            return false;
        }
        return timer_backend.run_due(now_ns, timer_drain_budget_, runner);
    };
    if (timer_kind_ == timer_kind::hierarchical_wheel) {
        return run_due(timer_wheel_, now, run);
    }
    return run_due(timer_heap_, now, run);
}

inline void runtime_executor::cancel_timers() noexcept {
    if (timer_kind_ == timer_kind::hierarchical_wheel) {
        timer_wheel_.cancel_all();
        return;
    }
    timer_heap_.cancel_all();
}

} // namespace af::detail
