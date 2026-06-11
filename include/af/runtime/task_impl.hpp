#pragma once

namespace af {

template <typename Rep, typename Period>
bool runtime_task::schedule_after(std::chrono::duration<Rep, Period> delay) noexcept {
    return schedule_after(runtime::current_thread_index(), delay);
}

template <typename Clock, typename Duration>
bool runtime_task::schedule_at(std::chrono::time_point<Clock, Duration> time) noexcept {
    return schedule_at(runtime::current_thread_index(), time);
}

template <typename Rep, typename Period>
task_result runtime_task::pending_after(std::chrono::duration<Rep, Period> delay) noexcept {
    return pending_after(runtime::current_thread_index(), delay);
}

template <typename Clock, typename Duration>
task_result runtime_task::pending_at(std::chrono::time_point<Clock, Duration> time) noexcept {
    return pending_at(runtime::current_thread_index(), time);
}

inline runtime_task::runtime_task(factory_token, runtime &owner) noexcept
    : owner_(&owner),
      task_id_(owner.config().diagnostics.enable_task_id ? allocate_task_id() : invalid_task_id) {}

inline bool runtime_task::schedule_to(std::uint16_t thread) noexcept {
    if (owner_ == nullptr || !owner_->valid_thread(thread)) {
        return fail_created_schedule_request();
    }

    for (;;) {
        const task_state state = state_.load(std::memory_order_acquire);
        switch (state) {
        case task_state::created:
        case task_state::pending:
            switch (enqueue_from_state(state, thread)) {
            case enqueue_result::queued:
                return true;
            case enqueue_result::retry:
                break;
            case enqueue_result::failed:
                return false;
            }
            break;
        case task_state::running:
            return request_schedule_after_running(thread);
        case task_state::queued:
        case task_state::timer_arming:
        case task_state::timer_pending:
        case task_state::starting:
        case task_state::done:
            AF_ASSERT(false && "task cannot be scheduled from its current state");
            return false;
        }
    }
}

inline bool runtime_task::schedule_after_ns(std::uint16_t thread,
                                            std::chrono::nanoseconds delay) noexcept {
    if (owner_ == nullptr || !owner_->valid_thread(thread)) {
        return fail_created_schedule_request();
    }

    const std::int64_t deadline_ns = timer_deadline_after(delay);
    for (;;) {
        const task_state state = state_.load(std::memory_order_acquire);
        switch (state) {
        case task_state::created:
        case task_state::pending:
            switch (enqueue_timer_from_state(state, thread, deadline_ns)) {
            case enqueue_result::queued:
                return true;
            case enqueue_result::retry:
                break;
            case enqueue_result::failed:
                return false;
            }
            break;
        case task_state::running:
            return request_timer_after_running(thread, deadline_ns);
        case task_state::queued:
        case task_state::timer_arming:
        case task_state::timer_pending:
        case task_state::starting:
        case task_state::done:
            AF_ASSERT(false && "task cannot be timer-scheduled from its current state");
            return false;
        }
    }
}

inline runtime_task::enqueue_result
runtime_task::enqueue_from_state(task_state previous, std::uint16_t thread) noexcept {
    task_state expected = previous;
    if (!state_.compare_exchange_weak(expected, task_state::queued, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
        return enqueue_result::retry;
    }

    const bool release_created_ref = previous == task_state::created;
    add_lifetime_ref();
    if (owner_->post(thread, this)) {
        if (release_created_ref) {
            release_lifetime_ref();
        }
        return enqueue_result::queued;
    }

    state_.store(release_created_ref ? task_state::done : previous, std::memory_order_release);
    if (release_created_ref) {
        release_lifetime_ref();
    }
    release_lifetime_ref();
    return enqueue_result::failed;
}

inline runtime_task::enqueue_result
runtime_task::enqueue_timer_from_state(task_state previous, std::uint16_t thread,
                                       std::int64_t deadline_ns) noexcept {
    timer_deadline_ns_ = deadline_ns;
    task_state expected = previous;
    if (!state_.compare_exchange_weak(expected, task_state::timer_arming, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
        timer_deadline_ns_ = no_timer_deadline_ns;
        return enqueue_result::retry;
    }

    const bool release_created_ref = previous == task_state::created;
    add_lifetime_ref();
    if (owner_->post(thread, this)) {
        if (release_created_ref) {
            release_lifetime_ref();
        }
        return enqueue_result::queued;
    }

    timer_deadline_ns_ = no_timer_deadline_ns;
    state_.store(release_created_ref ? task_state::done : previous, std::memory_order_release);
    if (release_created_ref) {
        release_lifetime_ref();
    }
    release_lifetime_ref();
    return enqueue_result::failed;
}

inline bool runtime_task::request_schedule_after_running(std::uint16_t thread) noexcept {
    const std::uint32_t requested = static_cast<std::uint32_t>(thread);
    std::uint32_t expected = no_requested_thread;
    if (requested_thread_.compare_exchange_strong(expected, requested, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
        return enqueue_late_request_after_running(thread, no_timer_deadline_ns);
    }
    if (expected == requested) {
        return true;
    }
    AF_ASSERT(false && "a running task can only request one next target thread");
    return false;
}

inline bool runtime_task::request_timer_after_running(std::uint16_t thread,
                                                      std::int64_t deadline_ns) noexcept {
    requested_deadline_ns_.store(deadline_ns, std::memory_order_relaxed);
    const std::uint32_t requested = static_cast<std::uint32_t>(thread);
    std::uint32_t expected = no_requested_thread;
    if (requested_thread_.compare_exchange_strong(expected, requested, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
        return enqueue_late_request_after_running(thread, deadline_ns);
    }
    if (expected == requested) {
        requested_deadline_ns_.store(deadline_ns, std::memory_order_release);
        return true;
    }
    requested_deadline_ns_.store(no_timer_deadline_ns, std::memory_order_relaxed);
    AF_ASSERT(false && "a running task can only request one next target thread");
    return false;
}

inline bool runtime_task::enqueue_late_request_after_running(std::uint16_t thread,
                                                             std::int64_t deadline_ns) noexcept {
    if (state_.load(std::memory_order_acquire) != task_state::pending) {
        return true;
    }

    std::uint32_t expected = static_cast<std::uint32_t>(thread);
    if (!requested_thread_.compare_exchange_strong(
            expected, no_requested_thread, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
    }

    if (deadline_ns != no_timer_deadline_ns) {
        requested_deadline_ns_.store(no_timer_deadline_ns, std::memory_order_release);
        return enqueue_timer_from_state(task_state::pending, thread, deadline_ns) ==
               enqueue_result::queued;
    }
    return enqueue_from_state(task_state::pending, thread) == enqueue_result::queued;
}

inline bool runtime_task::enqueue_next_from_running(std::uint16_t thread) noexcept {
    add_lifetime_ref();
    state_.store(task_state::queued, std::memory_order_release);
    if (owner_->post(thread, this)) {
        return true;
    }

    state_.store(task_state::done, std::memory_order_release);
    release_lifetime_ref();
    return false;
}

inline bool runtime_task::enqueue_timer_next_from_running(std::uint16_t thread,
                                                          std::int64_t deadline_ns) noexcept {
    add_lifetime_ref();
    timer_deadline_ns_ = deadline_ns;
    state_.store(task_state::timer_arming, std::memory_order_release);
    if (owner_->post(thread, this)) {
        return true;
    }

    timer_deadline_ns_ = no_timer_deadline_ns;
    state_.store(task_state::done, std::memory_order_release);
    release_lifetime_ref();
    return false;
}

inline bool runtime_task::fail_created_schedule_request() noexcept {
    task_state expected = task_state::created;
    if (state_.compare_exchange_strong(expected, task_state::done, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        release_lifetime_ref();
    }
    return false;
}

inline bool runtime_task::mark_timer_pending() noexcept {
    task_state expected = task_state::timer_arming;
    return state_.compare_exchange_strong(expected, task_state::timer_pending,
                                          std::memory_order_acq_rel, std::memory_order_acquire);
}

inline bool runtime_task::mark_timer_ready() noexcept {
    task_state expected = task_state::timer_pending;
    if (!state_.compare_exchange_strong(expected, task_state::queued, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        return false;
    }
    timer_deadline_ns_ = no_timer_deadline_ns;
    return true;
}

inline void runtime_task::cancel_timer() noexcept {
    task_state expected = task_state::timer_pending;
    if (!state_.compare_exchange_strong(expected, task_state::done, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        return;
    }
    timer_deadline_ns_ = no_timer_deadline_ns;
    requested_deadline_ns_.store(no_timer_deadline_ns, std::memory_order_relaxed);
    requested_thread_.store(no_requested_thread, std::memory_order_relaxed);
    release_lifetime_ref();
}

inline void runtime_task::finish_after_run(task_result result) noexcept {
    const std::uint32_t requested =
        requested_thread_.exchange(no_requested_thread, std::memory_order_acq_rel);
    const std::int64_t requested_deadline =
        requested_deadline_ns_.exchange(no_timer_deadline_ns, std::memory_order_acq_rel);

    switch (result) {
    case task_result::pending:
        if (requested != no_requested_thread) {
            if (requested_deadline != no_timer_deadline_ns) {
                static_cast<void>(enqueue_timer_next_from_running(
                    static_cast<std::uint16_t>(requested), requested_deadline));
            } else {
                static_cast<void>(enqueue_next_from_running(static_cast<std::uint16_t>(requested)));
            }
            return;
        }
        state_.store(task_state::pending, std::memory_order_release);
        {
            const std::uint32_t late_requested =
                requested_thread_.exchange(no_requested_thread, std::memory_order_acq_rel);
            const std::int64_t late_requested_deadline =
                requested_deadline_ns_.exchange(no_timer_deadline_ns, std::memory_order_acq_rel);
            if (late_requested != no_requested_thread) {
                if (late_requested_deadline != no_timer_deadline_ns) {
                    static_cast<void>(enqueue_timer_from_state(
                        task_state::pending, static_cast<std::uint16_t>(late_requested),
                        late_requested_deadline));
                } else {
                    static_cast<void>(enqueue_from_state(
                        task_state::pending, static_cast<std::uint16_t>(late_requested)));
                }
            }
        }
        return;
    case task_result::again:
        if (requested_deadline != no_timer_deadline_ns) {
            static_cast<void>(enqueue_timer_next_from_running(
                requested == no_requested_thread ? runtime::current_thread_index()
                                                 : static_cast<std::uint16_t>(requested),
                requested_deadline));
        } else {
            static_cast<void>(enqueue_next_from_running(
                requested == no_requested_thread ? runtime::current_thread_index()
                                                 : static_cast<std::uint16_t>(requested)));
        }
        return;
    case task_result::done:
    case task_result::failed:
    case task_result::cancelled:
        state_.store(task_state::done, std::memory_order_release);
        return;
    }
}

inline void runtime_task::run(runtime &owner) noexcept {
    if (&owner != owner_) {
        AF_ASSERT(false && "task executed by a different runtime");
        return;
    }

    if (state_.load(std::memory_order_acquire) == task_state::timer_arming) {
        owner.arm_timer_on_current_executor(this);
        return;
    }

    task_state expected = task_state::queued;
    if (!state_.compare_exchange_strong(expected, task_state::running, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        AF_ASSERT(false && "executor popped a task that was not queued");
        return;
    }

    const task_id_type previous_task_id = runtime::exchange_current_task_id(task_id_);
    task_result result = task_result::done;
    try {
        result = run_task();
    } catch (...) {
        AF_ASSERT(false && "runtime_task::run_task must not throw");
        result = task_result::failed;
    }
    static_cast<void>(runtime::exchange_current_task_id(previous_task_id));

    finish_after_run(result);
    release_lifetime_ref();
}

} // namespace af
