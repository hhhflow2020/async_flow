#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>

#include "af/detail/config.hpp"

namespace af {

template <typename TraitsT>
class AsyncRuntime;

enum class TaskResult : std::uint8_t {
    Done,
    Pending,
    Again,
    Failed,
    Cancelled,
};

enum class QueueFullPolicy : std::uint8_t {
    Reject,
    Yield,
};

enum class ShutdownPolicy : std::uint8_t {
    WaitForTasks,
    StopImmediately,
};

enum class ThreadKind : std::uint8_t {
    Worker,
    IoUring,
    Epoll,
};

enum class TaskState : std::uint8_t {
    Created,
    Queued,
    Running,
    Pending,
    Done,
};

inline constexpr std::uint32_t io_readable = 1U << 0U;
inline constexpr std::uint32_t io_writable = 1U << 1U;
inline constexpr std::uint32_t io_error = 1U << 2U;
inline constexpr std::uint32_t io_hangup = 1U << 3U;
inline constexpr std::uint32_t io_more = 1U << 4U;
inline constexpr std::uint32_t io_buffer_selected = 1U << 5U;
inline constexpr std::uint32_t io_buffer_id_shift = 16U;
inline constexpr std::uint32_t io_buffer_id_mask = 0xffff0000U;

struct IoResult {
    int fd{-1};
    std::uint32_t events{0};
    int error{0};
    std::int64_t result{0};

    [[nodiscard]] bool readable() const noexcept {
        return (events & io_readable) != 0U;
    }

    [[nodiscard]] bool writable() const noexcept {
        return (events & io_writable) != 0U;
    }

    [[nodiscard]] bool failed() const noexcept {
        return error != 0 || (events & (io_error | io_hangup)) != 0U;
    }

    [[nodiscard]] bool buffer_selected() const noexcept {
        return (events & io_buffer_selected) != 0U;
    }

    [[nodiscard]] std::uint16_t buffer_id() const noexcept {
        return static_cast<std::uint16_t>((events & io_buffer_id_mask) >> io_buffer_id_shift);
    }
};

enum class IoWaitKind : std::uint8_t {
    None,
    Readiness,
    Completion,
};

struct IoOpState {
    IoResult wait{};
    IoWaitKind wait_kind{IoWaitKind::None};
    bool waiting{false};
    bool readiness_rearm_hint{false};
    int readiness_fd{-1};

    void reset() noexcept {
        wait = IoResult{};
        wait_kind = IoWaitKind::None;
        waiting = false;
        readiness_rearm_hint = false;
        readiness_fd = -1;
    }
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

template <typename RuntimeT>
inline constexpr bool task_registry_enabled_v = [] {
    if constexpr (requires { RuntimeT::Traits::enable_task_registry; }) {
        return static_cast<bool>(RuntimeT::Traits::enable_task_registry);
    } else {
        return false;
    }
}();

template <typename TaskT>
struct TaskRegistryLinks {
    TaskT* prev{nullptr};
    TaskT* next{nullptr};
    bool registered{false};
};

struct EmptyTaskRegistryLinks {};

} // namespace detail

template <typename RuntimeT>
class BasicTask {
public:
    using Runtime = RuntimeT;
    using Thread = typename Runtime::Thread;
    using DestroyFn = void (*)(BasicTask*) noexcept;

    class FactoryToken {
    public:
        FactoryToken(const FactoryToken&) noexcept = default;
        FactoryToken& operator=(const FactoryToken&) = delete;

    private:
        constexpr FactoryToken() noexcept = default;

        template <typename TraitsT>
        friend class AsyncRuntime;
    };

    BasicTask() = delete;
    BasicTask(const BasicTask&) = delete;
    BasicTask& operator=(const BasicTask&) = delete;
    virtual ~BasicTask() = default;

    static void* operator new(std::size_t) = delete;
    static void* operator new[](std::size_t) = delete;
    static void* operator new(std::size_t, std::align_val_t) = delete;
    static void* operator new[](std::size_t, std::align_val_t) = delete;

protected:
    explicit BasicTask(FactoryToken) noexcept {}

    static void operator delete(void* ptr) noexcept {
        ::operator delete(ptr);
    }

    static void operator delete[](void* ptr) noexcept {
        ::operator delete[](ptr);
    }

    static void operator delete(void* ptr, std::align_val_t align) noexcept {
        ::operator delete(ptr, align);
    }

    static void operator delete[](void* ptr, std::align_val_t align) noexcept {
        ::operator delete[](ptr, align);
    }

    [[nodiscard]] bool schedule(Thread thread) noexcept {
        return Runtime::post(thread, this);
    }

    TaskResult pending_on(Thread thread) noexcept {
        if (!Runtime::post(thread, this)) {
            return TaskResult::Cancelled;
        }
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

    static TaskResult cancelled() noexcept {
        return TaskResult::Cancelled;
    }

    static bool runtime_stopping() noexcept {
        return Runtime::is_stopping();
    }

    static Thread current_thread() noexcept {
        return Runtime::current_thread();
    }

    static bool is_current(Thread thread) noexcept {
        return Runtime::current_thread() == thread;
    }

    [[nodiscard]] bool wait_io(Thread thread, int fd, std::uint32_t events, IoResult* result) noexcept {
        return Runtime::io_wait(thread, fd, events, this, result);
    }

    [[nodiscard]] std::uint32_t last_parallel_failures() const noexcept {
        return last_parallel_failures_;
    }

private:
    virtual TaskResult run() = 0;
    virtual void on_runtime_cancel() noexcept {}

    void set_destroy_fn(DestroyFn destroy_fn) noexcept {
        destroy_fn_ = destroy_fn;
    }

    void attach_start_handle() noexcept {
        add_lifetime_ref();
    }

    void destroy_self() noexcept {
        AF_ASSERT(destroy_fn_ != nullptr);
        destroy_fn_(this);
    }

    void add_lifetime_ref() noexcept {
        lifetime_refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void release_lifetime_ref() noexcept {
        if (lifetime_refs_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            destroy_self();
        }
    }

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
                AF_ASSERT(false && "task was scheduled more than once or after completion");
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
        AF_ASSERT(ok);
    }

    [[nodiscard]] bool is_created() const noexcept {
        return state_.load(std::memory_order_acquire) == TaskState::Created;
    }

    void set_last_parallel_failures(std::uint32_t failures) noexcept {
        last_parallel_failures_ = failures;
    }

    bool request_wake_while_running(std::uint16_t thread_index) noexcept {
        std::uint32_t expected = detail::no_requested_thread;
        const std::uint32_t desired = static_cast<std::uint32_t>(thread_index) + 1U;
        const bool ok = requested_thread_.compare_exchange_strong(
            expected,
            desired,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
        AF_ASSERT(ok && "a running task can only register one wake-up");
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
    std::atomic<std::uint32_t> lifetime_refs_{1};
    std::uint32_t last_parallel_failures_{0};
    DestroyFn destroy_fn_{nullptr};
    [[no_unique_address]] std::conditional_t<
        detail::task_registry_enabled_v<Runtime>,
        detail::TaskRegistryLinks<BasicTask>,
        detail::EmptyTaskRegistryLinks>
        registry_;

    template <typename TraitsT>
    friend class AsyncRuntime;
};

} // namespace af
