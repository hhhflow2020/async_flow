#pragma once

#include <atomic>
#include <cstdint>
#include <type_traits>

#include "caf/detail/config.hpp"

namespace caf {

template <typename TraitsT>
class AsyncRuntime;

enum class TaskResult : std::uint8_t {
    Done,
    Pending,
    Again,
    Failed,
};

enum class QueueFullPolicy : std::uint8_t {
    Reject,
    Yield,
};

enum class TaskState : std::uint8_t {
    Created,
    Queued,
    Running,
    Pending,
    Done,
};

namespace detail {

enum class ScheduleAction : std::uint8_t {
    Enqueue,
    Deferred,
    Rejected,
};

struct ScheduleRequest {
    ScheduleAction action{ScheduleAction::Rejected};
    TaskState previous{TaskState::Done};
};

inline constexpr std::uint32_t no_requested_thread = 0;

} // namespace detail

template <typename RuntimeT>
class BasicTask {
public:
    using Runtime = RuntimeT;
    using Thread = typename Runtime::Thread;

    BasicTask() = default;
    BasicTask(const BasicTask&) = delete;
    BasicTask& operator=(const BasicTask&) = delete;
    virtual ~BasicTask() = default;

protected:
    [[nodiscard]] bool schedule(Thread thread) noexcept {
        return Runtime::post(thread, this);
    }

    TaskResult pending_on(Thread thread) noexcept {
        [[maybe_unused]] const bool ok = Runtime::post(thread, this);
        CAF_ASSERT(ok);
        return TaskResult::Pending;
    }

    static TaskResult done() noexcept {
        return TaskResult::Done;
    }

    static TaskResult pending() noexcept {
        return TaskResult::Pending;
    }

    static TaskResult again() noexcept {
        return TaskResult::Again;
    }

    static TaskResult failed() noexcept {
        return TaskResult::Failed;
    }

    static Thread current_thread() noexcept {
        return Runtime::current_thread();
    }

    static bool is_current(Thread thread) noexcept {
        return Runtime::current_thread() == thread;
    }

private:
    virtual TaskResult run() = 0;

    detail::ScheduleRequest request_schedule(std::uint16_t thread_index) noexcept {
        for (;;) {
            TaskState state = state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Created:
            case TaskState::Pending: {
                TaskState expected = state;
                if (state_.compare_exchange_weak(
                        expected,
                        TaskState::Queued,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    return {detail::ScheduleAction::Enqueue, state};
                }
                break;
            }

            case TaskState::Running:
                return request_wake_while_running(thread_index)
                    ? detail::ScheduleRequest{detail::ScheduleAction::Deferred, state}
                    : detail::ScheduleRequest{detail::ScheduleAction::Rejected, state};

            case TaskState::Queued:
            case TaskState::Done:
                CAF_ASSERT(false && "task was scheduled more than once or after completion");
                return {detail::ScheduleAction::Rejected, state};
            }
        }
    }

    void cancel_schedule(TaskState previous) noexcept {
        TaskState expected = TaskState::Queued;
        [[maybe_unused]] const bool ok = state_.compare_exchange_strong(
            expected,
            previous,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
        CAF_ASSERT(ok);
    }

    [[nodiscard]] bool is_created() const noexcept {
        return state_.load(std::memory_order_acquire) == TaskState::Created;
    }

    bool request_wake_while_running(std::uint16_t thread_index) noexcept {
        std::uint32_t expected = detail::no_requested_thread;
        const std::uint32_t desired = static_cast<std::uint32_t>(thread_index) + 1U;
        const bool ok = requested_thread_.compare_exchange_strong(
            expected,
            desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
        CAF_ASSERT(ok && "a running task can only register one wake-up");
        return ok;
    }

    std::uint16_t take_requested_thread() noexcept {
        const std::uint32_t value = requested_thread_.exchange(
            detail::no_requested_thread,
            std::memory_order_acq_rel);
        if (value == detail::no_requested_thread) {
            return Runtime::invalid_thread_index;
        }
        return static_cast<std::uint16_t>(value - 1U);
    }

    std::atomic<TaskState> state_{TaskState::Created};
    std::atomic<std::uint32_t> requested_thread_{detail::no_requested_thread};

    template <typename TraitsT>
    friend class AsyncRuntime;
};

} // namespace caf
