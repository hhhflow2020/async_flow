#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_scheduler.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::mark_ready(std::uint16_t source) noexcept {
    ready_sources_.mark(source);
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::notify_external_ready() noexcept {
    if (!external_ready_.load(std::memory_order_acquire)) {
        external_ready_.store(true, std::memory_order_release);
        notify_force();
        return;
    }
    notify();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::try_push_local(Task *task) noexcept {
    if (local_size_ == local_queue_.size()) {
        return false;
    }

    local_queue_[local_tail_ & (local_queue_.size() - 1U)] = task;
    ++local_tail_;
    ++local_size_;
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] typename Executor<RuntimeT, TraitsT>::Task *
Executor<RuntimeT, TraitsT>::try_pop_local() noexcept {
    if (local_size_ == 0) {
        return nullptr;
    }

    Task *task = local_queue_[local_head_ & (local_queue_.size() - 1U)];
    ++local_head_;
    --local_size_;
    return task;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::execute(Task *task) noexcept {
    TaskState expected = TaskState::Queued;
    if (!task->state_.compare_exchange_strong(
            expected, TaskState::Starting, std::memory_order_acq_rel, std::memory_order_acquire)) {
        AF_ASSERT(false && "executor popped a task that was not queued");
        return;
    }
    task->prepare_running_epoch();
    task->state_.store(TaskState::Running, std::memory_order_release);

    TaskResult result = TaskResult::Done;
    Task *previous_running_task = running_task_;
    running_task_ = task;
    try {
        result = task->run();
    } catch (...) {
        AF_ASSERT(false && "task::run must not throw");
        result = TaskResult::Done;
    }
    running_task_ = previous_running_task;

    switch (result) {
    case TaskResult::Done:
        finish_done(task);
        break;
    case TaskResult::Pending:
        finish_pending(task);
        break;
    case TaskResult::Again:
        finish_again(task);
        break;
    case TaskResult::Failed:
        finish_done(task);
        break;
    case TaskResult::Cancelled:
        finish_done(task);
        break;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::notify_force() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (notify_native_io_backend()) {
        return;
    }
    wake_epoch_.notify_one();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::run_loop() noexcept {
    RuntimeT::current_thread_index_ = index_;

    for (;;) {
        bool did_work = false;
        while (Task *task = pop_one()) {
            did_work = true;
            execute(task);
        }

#if defined(__linux__)
        if (flush_io_uring_submissions_or_fail()) {
            did_work = true;
        }
#endif

        if (stop_requested_.load(std::memory_order_acquire)) {
            if (!did_work) {
                break;
            }
            continue;
        }

        if (poll_io(0)) {
            continue;
        }

        const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
        sleeping_.store(true, std::memory_order_release);
        if (stop_requested_.load(std::memory_order_acquire)) {
            sleeping_.store(false, std::memory_order_relaxed);
            continue;
        }

        if (Task *task = pop_one()) {
            sleeping_.store(false, std::memory_order_relaxed);
            execute(task);
        } else {
            if (wake_epoch_.load(std::memory_order_acquire) != observed) {
                sleeping_.store(false, std::memory_order_relaxed);
                continue;
            }
            if (io_thread() && io_backend_available()) {
                static_cast<void>(poll_io(-1));
            } else {
                wake_epoch_.wait(observed, std::memory_order_acquire);
            }
            sleeping_.store(false, std::memory_order_relaxed);
        }
    }

    RuntimeT::current_thread_index_ = invalid_thread_index;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::advance_ready_word_cursor_after(std::size_t word) noexcept {
    if constexpr (decltype(ready_sources_)::word_count > 1U) {
        std::size_t next = word + 1U;
        if (next == decltype(ready_sources_)::word_count) {
            next = 0;
        }
        next_ready_word_ = static_cast<std::uint16_t>(next);
    } else {
        static_cast<void>(word);
    }
}

template <typename RuntimeT, typename TraitsT>
typename Executor<RuntimeT, TraitsT>::Task *Executor<RuntimeT, TraitsT>::pop_one() noexcept {
    if (Task *task = try_pop_local()) {
        return task;
    }

    for (std::size_t checked_word = 0; checked_word < decltype(ready_sources_)::word_count;
         ++checked_word) {
        std::size_t word = checked_word;
        if constexpr (decltype(ready_sources_)::word_count > 1U) {
            word += next_ready_word_;
            if (word >= decltype(ready_sources_)::word_count) {
                word -= decltype(ready_sources_)::word_count;
            }
        }
        std::uint64_t mask = ready_sources_.load_word(word);
        while (mask != 0U) {
            const std::uint16_t source = static_cast<std::uint16_t>(
                decltype(ready_sources_)::word_base(word) + std::countr_zero(mask));
            const std::uint64_t bit = 1ULL << (source & 63U);
            mask &= ~bit;
            if (source == index_) {
                continue;
            }
            if (Task *task = spsc_queue(source, index_).try_pop()) {
                advance_ready_word_cursor_after(word);
                next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                return task;
            }
            ready_sources_.clear(source);
            if (Task *task = spsc_queue(source, index_).try_pop()) {
                advance_ready_word_cursor_after(word);
                next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                mark_ready(source);
                return task;
            }
        }
    }

    for (std::uint16_t checked = 0; checked < thread_count; ++checked) {
        const std::uint16_t source =
            static_cast<std::uint16_t>((next_source_ + checked) % thread_count);
        if (source == index_) {
            continue;
        }
        if (Task *task = spsc_queue(source, index_).try_pop()) {
            next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
            mark_ready(source);
            return task;
        }
    }

    if (external_ready_.load(std::memory_order_acquire)) {
        if (Task *task = RuntimeT::external_queues_[index_]->try_pop()) {
            return task;
        }

        external_ready_.store(false, std::memory_order_release);
        if (Task *task = RuntimeT::external_queues_[index_]->try_pop()) {
            external_ready_.store(true, std::memory_order_release);
            return task;
        }
    }

    return RuntimeT::external_queues_[index_]->try_pop();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_done(Task *task) noexcept {
    task->state_.store(TaskState::Done, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    on_task_finished(task);
    task->release_lifetime_ref();
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_pending(Task *task) noexcept {
    task->state_.store(TaskState::Pending, std::memory_order_release);
    const std::uint16_t requested = task->take_requested_thread();
    if (requested != invalid_thread_index) {
        // A running-task wake request is converted into a real queue entry only
        // if the task is still Pending; a concurrent Pending->Queued wake wins
        // otherwise.
        enqueue_pending_blocking(requested, task);
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::finish_again(Task *task) noexcept {
    task->state_.store(TaskState::Queued, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    enqueue_ready_blocking_from_runtime_thread(index_, index_, task);
}

} // namespace af::detail
