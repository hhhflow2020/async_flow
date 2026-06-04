#pragma once

namespace af {

inline runtime_task::runtime_task(factory_token, runtime &owner) noexcept
    : owner_(&owner), task_id_(allocate_task_id()) {}

inline bool runtime_task::schedule_to(std::uint16_t thread) noexcept {
    if (owner_ == nullptr || !owner_->valid_thread(thread)) {
        return false;
    }

    for (;;) {
        const task_state state = state_.load(std::memory_order_acquire);
        switch (state) {
        case task_state::created:
        case task_state::pending:
            if (enqueue_from_state(state, thread)) {
                return true;
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

inline bool runtime_task::enqueue_from_state(task_state previous, std::uint16_t thread) noexcept {
    task_state expected = previous;
    if (!state_.compare_exchange_weak(expected, task_state::queued, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
        return false;
    }

    add_lifetime_ref();
    if (owner_->post(thread, this)) {
        return true;
    }

    state_.store(previous, std::memory_order_release);
    release_lifetime_ref();
    return false;
}

inline bool runtime_task::request_schedule_after_running(std::uint16_t thread) noexcept {
    const std::uint32_t requested = static_cast<std::uint32_t>(thread);
    std::uint32_t expected = no_requested_thread;
    if (requested_thread_.compare_exchange_strong(expected, requested, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
        return true;
    }
    if (expected == requested) {
        return true;
    }
    AF_ASSERT(false && "a running task can only request one next target thread");
    return false;
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

inline void runtime_task::finish_after_run(task_result result) noexcept {
    const std::uint32_t requested =
        requested_thread_.exchange(no_requested_thread, std::memory_order_acq_rel);

    switch (result) {
    case task_result::pending:
        if (requested != no_requested_thread) {
            static_cast<void>(enqueue_next_from_running(static_cast<std::uint16_t>(requested)));
            return;
        }
        state_.store(task_state::pending, std::memory_order_release);
        return;
    case task_result::again:
        static_cast<void>(enqueue_next_from_running(requested == no_requested_thread
                                                        ? runtime::current_thread_index()
                                                        : static_cast<std::uint16_t>(requested)));
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
