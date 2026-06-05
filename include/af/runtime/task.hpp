#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/config.hpp"
#include "af/runtime/config_types.hpp"
#include "af/runtime/work.hpp"
#include "af/task.hpp"

namespace af {

class runtime;
class runtime_task;

namespace detail {
struct runtime_task_access;
[[nodiscard]] const task_pool_config &runtime_task_pool_config(const runtime &owner) noexcept;
[[noreturn]] void handle_runtime_task_bad_alloc(const runtime &owner);
} // namespace detail

using runtime_task_id = std::uint64_t;
inline constexpr runtime_task_id runtime_invalid_task_id = 0;

template <typename TaskT> class runtime_task_handle;

template <typename TaskT, typename... Args>
[[nodiscard]] runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

template <typename TaskT, typename... Args>
[[nodiscard]] runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;

class runtime_task : public runtime_work {
public:
    using task_id_type = runtime_task_id;
    using destroy_fn = void (*)(runtime_task *) noexcept;
    static constexpr task_id_type invalid_task_id = runtime_invalid_task_id;

    class factory_token {
    public:
        factory_token(const factory_token &) noexcept = default;
        factory_token &operator=(const factory_token &) = delete;

    private:
        constexpr factory_token() noexcept = default;

        template <typename TaskT, typename... Args>
        friend runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

        template <typename TaskT, typename... Args>
        friend runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;
    };

    runtime_task() = delete;
    runtime_task(const runtime_task &) = delete;
    runtime_task &operator=(const runtime_task &) = delete;
    ~runtime_task() override = default;

    [[nodiscard]] task_id_type task_id() const noexcept {
        return task_id_;
    }

    [[nodiscard]] runtime &owner() noexcept {
        return *owner_;
    }

    [[nodiscard]] const runtime &owner() const noexcept {
        return *owner_;
    }

protected:
    explicit runtime_task(factory_token, runtime &owner) noexcept;

    [[nodiscard]] bool schedule(std::uint16_t thread) noexcept {
        return schedule_to(thread);
    }

    [[nodiscard]] bool schedule(thread_ref thread) noexcept {
        return schedule_to(thread);
    }

    [[nodiscard]] bool schedule_to(std::uint16_t thread) noexcept;

    [[nodiscard]] bool schedule_to(thread_ref thread) noexcept {
        return schedule_to(thread.index);
    }

    template <typename Rep, typename Period>
    [[nodiscard]] bool schedule_after(std::uint16_t thread,
                                      std::chrono::duration<Rep, Period> delay) noexcept {
        return schedule_after_ns(thread, normalize_delay(delay));
    }

    template <typename Rep, typename Period>
    [[nodiscard]] bool schedule_after(thread_ref thread,
                                      std::chrono::duration<Rep, Period> delay) noexcept {
        return schedule_after(thread.index, delay);
    }

    template <typename Rep, typename Period>
    [[nodiscard]] bool schedule_after(std::chrono::duration<Rep, Period> delay) noexcept;

    template <typename Clock, typename Duration>
    [[nodiscard]] bool schedule_at(std::uint16_t thread,
                                   std::chrono::time_point<Clock, Duration> time) noexcept {
        return schedule_after_ns(thread, delay_until(time));
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool schedule_at(thread_ref thread,
                                   std::chrono::time_point<Clock, Duration> time) noexcept {
        return schedule_at(thread.index, time);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool schedule_at(std::chrono::time_point<Clock, Duration> time) noexcept;

    task_result pending_to(std::uint16_t thread) noexcept {
        return schedule_to(thread) ? task_result::pending : task_result::cancelled;
    }

    task_result pending_to(thread_ref thread) noexcept {
        return pending_to(thread.index);
    }

    task_result pending(std::uint16_t thread) noexcept {
        return pending_to(thread);
    }

    task_result pending(thread_ref thread) noexcept {
        return pending_to(thread);
    }

    template <typename Rep, typename Period>
    task_result pending_after(std::uint16_t thread,
                              std::chrono::duration<Rep, Period> delay) noexcept {
        return schedule_after(thread, delay) ? task_result::pending : task_result::cancelled;
    }

    template <typename Rep, typename Period>
    task_result pending_after(thread_ref thread,
                              std::chrono::duration<Rep, Period> delay) noexcept {
        return pending_after(thread.index, delay);
    }

    template <typename Rep, typename Period>
    task_result pending_after(std::chrono::duration<Rep, Period> delay) noexcept;

    template <typename Clock, typename Duration>
    task_result pending_at(std::uint16_t thread,
                           std::chrono::time_point<Clock, Duration> time) noexcept {
        return schedule_at(thread, time) ? task_result::pending : task_result::cancelled;
    }

    template <typename Clock, typename Duration>
    task_result pending_at(thread_ref thread,
                           std::chrono::time_point<Clock, Duration> time) noexcept {
        return pending_at(thread.index, time);
    }

    template <typename Clock, typename Duration>
    task_result pending_at(std::chrono::time_point<Clock, Duration> time) noexcept;

    [[nodiscard]] static constexpr task_result done() noexcept {
        return task_result::done;
    }

    [[nodiscard]] static constexpr task_result pending() noexcept {
        return task_result::pending;
    }

    [[nodiscard]] static constexpr task_result again() noexcept {
        return task_result::again;
    }

    [[nodiscard]] static constexpr task_result reschedule() noexcept {
        return task_result::again;
    }

    [[nodiscard]] static constexpr task_result failed() noexcept {
        return task_result::failed;
    }

    [[nodiscard]] static constexpr task_result cancelled() noexcept {
        return task_result::cancelled;
    }

    [[nodiscard]] std::uint32_t last_parallel_failures() const noexcept {
        return last_parallel_failures_;
    }

private:
    static constexpr std::uint32_t no_requested_thread = std::numeric_limits<std::uint32_t>::max();
    static constexpr task_id_type task_id_block_size = 1024;
    static constexpr std::int64_t no_timer_deadline_ns = 0;

    virtual task_result run_task() noexcept = 0;

    void run(runtime &owner) noexcept final;
    void set_destroy_fn(destroy_fn destroy) noexcept {
        destroy_ = destroy;
    }

    void release_handle() noexcept {
        release_lifetime_ref();
    }

    void add_lifetime_ref() noexcept {
        lifetime_refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void release_lifetime_ref() noexcept {
        if (lifetime_refs_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            AF_ASSERT(destroy_ != nullptr);
            destroy_(this);
        }
    }

    [[nodiscard]] bool request_schedule_after_running(std::uint16_t thread) noexcept;
    [[nodiscard]] bool request_timer_after_running(std::uint16_t thread,
                                                   std::int64_t deadline_ns) noexcept;
    [[nodiscard]] bool enqueue_late_request_after_running(std::uint16_t thread,
                                                          std::int64_t deadline_ns) noexcept;
    [[nodiscard]] bool enqueue_from_state(task_state previous, std::uint16_t thread) noexcept;
    [[nodiscard]] bool enqueue_timer_from_state(task_state previous, std::uint16_t thread,
                                                std::int64_t deadline_ns) noexcept;
    [[nodiscard]] bool enqueue_next_from_running(std::uint16_t thread) noexcept;
    [[nodiscard]] bool enqueue_timer_next_from_running(std::uint16_t thread,
                                                       std::int64_t deadline_ns) noexcept;
    [[nodiscard]] bool mark_timer_pending() noexcept;
    [[nodiscard]] bool mark_timer_ready() noexcept;
    void cancel_timer() noexcept;
    void finish_after_run(task_result result) noexcept;
    [[nodiscard]] bool schedule_after_ns(std::uint16_t thread,
                                         std::chrono::nanoseconds delay) noexcept;

    [[nodiscard]] static task_id_type allocate_task_id() noexcept {
        thread_local task_id_type next_local_task_id = invalid_task_id;
        thread_local task_id_type local_task_id_limit = invalid_task_id;
        if (next_local_task_id == local_task_id_limit) [[unlikely]] {
            next_local_task_id =
                next_task_id_.fetch_add(task_id_block_size, std::memory_order_relaxed);
            local_task_id_limit = next_local_task_id + task_id_block_size;
        }
        return next_local_task_id++;
    }

    template <typename Rep, typename Period>
    [[nodiscard]] static std::chrono::nanoseconds
    normalize_delay(std::chrono::duration<Rep, Period> delay) noexcept {
        if (delay <= delay.zero()) {
            return std::chrono::nanoseconds(0);
        }
        return std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] static std::chrono::nanoseconds
    delay_until(std::chrono::time_point<Clock, Duration> time) noexcept {
        const auto now = Clock::now();
        if (time <= now) {
            return std::chrono::nanoseconds(0);
        }
        return normalize_delay(time - now);
    }

    [[nodiscard]] static std::int64_t steady_now_ns() noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    [[nodiscard]] static std::int64_t
    timer_deadline_after(std::chrono::nanoseconds delay) noexcept {
        const std::int64_t now = steady_now_ns();
        const std::int64_t count = delay.count();
        if (count <= 0) {
            return now;
        }
        if (count > std::numeric_limits<std::int64_t>::max() - now) {
            return std::numeric_limits<std::int64_t>::max();
        }
        return now + count;
    }

    runtime *owner_{nullptr};
    std::atomic<task_state> state_{task_state::created};
    std::atomic<std::uint32_t> requested_thread_{no_requested_thread};
    std::atomic<std::int64_t> requested_deadline_ns_{no_timer_deadline_ns};
    std::atomic<std::uint32_t> lifetime_refs_{1};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<task_id_type> next_task_id_{
        1};
    task_id_type task_id_{invalid_task_id};
    std::int64_t timer_deadline_ns_{no_timer_deadline_ns};
    destroy_fn destroy_{nullptr};
    std::uint32_t last_parallel_failures_{0};

    template <typename TaskT> friend class runtime_task_handle;

    template <typename TaskT, typename... Args>
    friend runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

    template <typename TaskT, typename... Args>
    friend runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;

    friend class runtime;
    friend struct detail::runtime_task_access;
};

namespace detail {

struct runtime_task_access {
    [[nodiscard]] static bool mark_timer_pending(runtime_task *task) noexcept {
        return task->mark_timer_pending();
    }

    [[nodiscard]] static bool mark_timer_ready(runtime_task *task) noexcept {
        return task->mark_timer_ready();
    }

    [[nodiscard]] static std::int64_t timer_deadline_ns(const runtime_task *task) noexcept {
        return task->timer_deadline_ns_;
    }

    static void cancel_timer(runtime_task *task) noexcept {
        if (task != nullptr) {
            task->cancel_timer();
        }
    }

    static void set_last_parallel_failures(runtime_task *task, std::uint32_t failures) noexcept {
        if (task != nullptr) {
            task->last_parallel_failures_ = failures;
        }
    }

    [[nodiscard]] static bool schedule_to(runtime_task *task, std::uint16_t thread) noexcept {
        return task != nullptr && task->schedule_to(thread);
    }

    static void add_lifetime_ref(runtime_task *task) noexcept {
        if (task != nullptr) {
            task->add_lifetime_ref();
        }
    }

    static void release_lifetime_ref(runtime_task *task) noexcept {
        if (task != nullptr) {
            task->release_lifetime_ref();
        }
    }
};

} // namespace detail

template <typename TaskT> class [[nodiscard]] runtime_task_handle {
public:
    runtime_task_handle() noexcept = default;
    explicit runtime_task_handle(TaskT *task) noexcept : task_(task) {}

    runtime_task_handle(const runtime_task_handle &) = delete;
    runtime_task_handle &operator=(const runtime_task_handle &) = delete;

    runtime_task_handle(runtime_task_handle &&other) noexcept
        : task_(std::exchange(other.task_, nullptr)) {}

    runtime_task_handle &operator=(runtime_task_handle &&other) noexcept {
        if (this != &other) {
            reset();
            task_ = std::exchange(other.task_, nullptr);
        }
        return *this;
    }

    ~runtime_task_handle() {
        reset();
    }

    [[nodiscard]] TaskT *get() const noexcept {
        return task_;
    }

    [[nodiscard]] TaskT &operator*() const noexcept {
        AF_ASSERT(task_ != nullptr);
        return *task_;
    }

    [[nodiscard]] TaskT *operator->() const noexcept {
        AF_ASSERT(task_ != nullptr);
        return task_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return task_ != nullptr;
    }

    void reset() noexcept {
        if (task_ != nullptr) {
            task_->release_handle();
            task_ = nullptr;
        }
    }

private:
    TaskT *task_{nullptr};
};

} // namespace af

#include "af/runtime/detail/task_pool.hpp"

namespace af {

template <typename TaskT, typename... Args>
runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args) {
    static_assert(std::is_base_of_v<runtime_task, TaskT>,
                  "TaskT must derive from af::runtime_task");
    const task_pool_config &pool_config = detail::runtime_task_pool_config(owner);
    return detail::visit_runtime_task_pool_holder<TaskT>(
        pool_config.local_cache_size, [&](auto &holder) -> runtime_task_handle<TaskT> {
            try {
                holder.reserve_at_least(pool_config.slab_object_count);
            } catch (const std::bad_alloc &) {
                detail::handle_runtime_task_bad_alloc(owner);
            }

            TaskT *task = holder.pool.create_with_oom_handler(
                [&owner] { detail::handle_runtime_task_bad_alloc(owner); },
                runtime_task::factory_token{}, owner, std::forward<Args>(args)...);
            task->set_destroy_fn(&detail::destroy_runtime_task<
                                 TaskT, std::decay_t<decltype(holder)>::local_cache_capacity>);
            return runtime_task_handle<TaskT>(task);
        });
}

template <typename TaskT, typename... Args>
runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept {
    static_assert(std::is_base_of_v<runtime_task, TaskT>,
                  "TaskT must derive from af::runtime_task");
    const task_pool_config &pool_config = detail::runtime_task_pool_config(owner);
    return detail::visit_runtime_task_pool_holder<TaskT>(
        pool_config.local_cache_size, [&](auto &holder) -> runtime_task_handle<TaskT> {
            if (!holder.try_reserve_at_least(pool_config.slab_object_count)) {
                return runtime_task_handle<TaskT>();
            }
            auto *task = holder.pool.try_create(runtime_task::factory_token{}, owner,
                                                std::forward<Args>(args)...);
            if (task == nullptr) {
                return runtime_task_handle<TaskT>();
            }
            task->set_destroy_fn(&detail::destroy_runtime_task<
                                 TaskT, std::decay_t<decltype(holder)>::local_cache_capacity>);
            return runtime_task_handle<TaskT>(task);
        });
}

} // namespace af
