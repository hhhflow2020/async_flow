#pragma once

#include "af/detail/log/absl_log_sink.hpp"

namespace af {

inline reactor *runtime::current_reactor() noexcept {
    if (current_executor_ == nullptr) {
        return nullptr;
    }
    return current_executor_->reactor_backend();
}

inline bool runtime::register_reactor_source(thread_index thread,
                                             fd_event_source *source) noexcept {
    if (!valid_thread(thread) || thread_kind_of(thread) != thread_kind::io || source == nullptr) {
        return false;
    }
    AF_ASSERT(current_runtime_ == this && current_thread_index_ == thread &&
              "reactor source registration must run on the owner IO runtime thread");
    if (current_runtime_ != this || current_thread_index_ != thread ||
        current_executor_ == nullptr) {
        return false;
    }
    reactor *backend = current_executor_->reactor_backend();
    return backend != nullptr && backend->add(source);
}

inline bool runtime::update_reactor_source(thread_index thread, fd_event_source *source) noexcept {
    if (!valid_thread(thread) || thread_kind_of(thread) != thread_kind::io || source == nullptr) {
        return false;
    }
    AF_ASSERT(current_runtime_ == this && current_thread_index_ == thread &&
              "reactor source update must run on the owner IO runtime thread");
    if (current_runtime_ != this || current_thread_index_ != thread ||
        current_executor_ == nullptr) {
        return false;
    }
    reactor *backend = current_executor_->reactor_backend();
    return backend != nullptr && backend->mod(source);
}

inline bool runtime::unregister_reactor_source(thread_index thread,
                                               fd_event_source *source) noexcept {
    if (!valid_thread(thread) || thread_kind_of(thread) != thread_kind::io || source == nullptr) {
        return false;
    }
    AF_ASSERT(current_runtime_ == this && current_thread_index_ == thread &&
              "reactor source unregistration must run on the owner IO runtime thread");
    if (current_runtime_ != this || current_thread_index_ != thread ||
        current_executor_ == nullptr) {
        return false;
    }
    reactor *backend = current_executor_->reactor_backend();
    return backend != nullptr && backend->del(source);
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

inline runtime::~runtime() {
    stop();
}

inline std::string runtime::status_message(runtime_config_validation_result validation) {
    std::string result("invalid af::runtime_config: ");
    result.append(runtime_config_status_name(validation.status));
    result.append(" at index ");
    result.append(std::to_string(validation.index));
    return result;
}

inline bool runtime::start() {
    runtime_state expected = runtime_state::stopped;
    if (!state_.compare_exchange_strong(expected, runtime_state::starting,
                                        std::memory_order_acq_rel, std::memory_order_acquire)) {
        return false;
    }

    try {
        executors_.clear();
        executors_.reserve(resolution_.resolved.threads.size());
        ordered_batch_state_.assign(resolution_.resolved.threads.size(), ordered_batch_state{});
        for (const auto &thread : resolution_.resolved.threads) {
            executors_.push_back(std::make_unique<detail::runtime_executor>(*this, thread));
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
        ordered_batch_state_.clear();
        active_thread_count_.store(0, std::memory_order_release);
        state_.store(runtime_state::stopped, std::memory_order_release);
        throw;
    }
}

inline bool runtime::flush_logger(std::chrono::milliseconds timeout) noexcept {
    return owned_logger_ == nullptr || owned_logger_->flush(timeout);
}

inline void runtime::stop() noexcept {
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
                                         std::memory_order_acq_rel, std::memory_order_acquire)) {
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
    ordered_batch_state_.clear();
    active_thread_count_.store(0, std::memory_order_release);
    active_work_count_.store(0, std::memory_order_release);
    state_.store(runtime_state::stopped, std::memory_order_release);
}

inline void runtime::request_stop() noexcept {
    for (auto &executor : executors_) {
        executor->request_stop();
    }
}

inline void runtime::join_all() noexcept {
    for (auto &executor : executors_) {
        executor->join();
    }
}

inline bool runtime::has_active_work() const noexcept {
    return active_work_count_.load(std::memory_order_acquire) != 0U;
}

inline void runtime::track_work_started() noexcept {
    active_work_count_.fetch_add(1, std::memory_order_acq_rel);
}

inline void runtime::track_work_finished() noexcept {
    const std::uint32_t previous = active_work_count_.fetch_sub(1, std::memory_order_acq_rel);
    AF_ASSERT(previous != 0U);
}

inline bool runtime::can_post_from_stopping_runtime_thread() const noexcept {
    return current_runtime_ == this && current_executor_ != nullptr;
}

inline bool runtime::try_enter_post() noexcept {
    runtime_state observed = state_.load(std::memory_order_acquire);
    if (observed != runtime_state::running) {
        if (observed != runtime_state::stopping || !can_post_from_stopping_runtime_thread()) {
            return false;
        }
        posting_count_.fetch_add(1, std::memory_order_acq_rel);
        observed = state_.load(std::memory_order_acquire);
        if (observed == runtime_state::stopping && can_post_from_stopping_runtime_thread()) {
            return true;
        }
        leave_post();
        return false;
    }
    posting_count_.fetch_add(1, std::memory_order_acq_rel);
    observed = state_.load(std::memory_order_acquire);
    if (observed == runtime_state::running ||
        (observed == runtime_state::stopping && can_post_from_stopping_runtime_thread())) {
        return true;
    }
    leave_post();
    return false;
}

inline void runtime::leave_post() noexcept {
    const std::uint32_t previous = posting_count_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1) {
        detail::atomic_notify_all(posting_count_);
    }
}

inline void runtime::wait_for_posts() noexcept {
    for (;;) {
        const std::uint32_t observed = posting_count_.load(std::memory_order_acquire);
        if (observed == 0) {
            return;
        }
        detail::atomic_wait_value(posting_count_, observed, std::memory_order_acquire);
    }
}

inline void runtime::on_executor_started() noexcept {
    active_thread_count_.fetch_add(1, std::memory_order_acq_rel);
    active_epoch_.fetch_add(1, std::memory_order_release);
    detail::atomic_notify_all(active_epoch_);
}

inline void runtime::on_executor_stopped() noexcept {
    active_thread_count_.fetch_sub(1, std::memory_order_acq_rel);
    active_epoch_.fetch_add(1, std::memory_order_release);
    detail::atomic_notify_all(active_epoch_);
}

inline void runtime::arm_timer_on_current_executor(runtime_task *task) noexcept {
    if (current_runtime_ != this || current_executor_ == nullptr) {
        detail::runtime_task_access::cancel_timer(task);
        return;
    }
    current_executor_->arm_timer(task);
}

inline runtime::task_id_type runtime::exchange_current_task_id(task_id_type next) noexcept {
    const task_id_type previous = current_task_id_;
    current_task_id_ = next;
    return previous;
}

inline void runtime::start_owned_logger_if_configured() {
    if (owned_logger_ != nullptr || config().logger.backends.empty()) {
        return;
    }
    owned_logger_stop_started_.store(false, std::memory_order_release);
    owned_logger_ = start_runtime_logging(*this);
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
    owned_logger_->stop(
        std::chrono::duration_cast<std::chrono::milliseconds>(config().shutdown.log_flush_timeout));
    owned_logger_.reset();
}

} // namespace af
