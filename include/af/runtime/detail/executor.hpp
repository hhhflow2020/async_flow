#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "af/detail/config.hpp"
#include "af/queue/intrusive_mpsc_queue.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/runtime/config_resolution.hpp"
#include "af/runtime/detail/timer_backend.hpp"
#include "af/reactor/reactor.hpp"
#include "af/runtime/work.hpp"

namespace af {

class runtime;
class runtime_task;

namespace detail {

class runtime_executor {
public:
    runtime_executor(runtime &owner, runtime_thread_info thread);
    runtime_executor(const runtime_executor &) = delete;
    runtime_executor &operator=(const runtime_executor &) = delete;
    ~runtime_executor();

    void start();
    void request_stop() noexcept;
    void notify() noexcept;
    void enqueue(runtime_work *work) noexcept;
    void arm_timer(runtime_task *task) noexcept;
    void join() noexcept;

    [[nodiscard]] reactor *reactor_backend() noexcept;
    [[nodiscard]] bool register_service_task(runtime_service_task *service) noexcept;
    [[nodiscard]] bool unregister_service_task(runtime_service_task *service) noexcept;

private:
    void run_loop() noexcept;
    [[nodiscard]] bool drain_inbox() noexcept;
    [[nodiscard]] bool drain_inbox_by_budget() noexcept;
    [[nodiscard]] bool drain_inbox_with_time_slice() noexcept;
    [[nodiscard]] bool run_service_tasks() noexcept;
    void run_work(runtime_work *work) noexcept;
    void run_unqueued_work(runtime_work *work) noexcept;
    [[nodiscard]] static std::int64_t steady_now_ns() noexcept;
    [[nodiscard]] std::chrono::nanoseconds timer_wait_duration() const noexcept;
    void wait_for_wake_or_timer(std::uint32_t observed) noexcept;
    [[nodiscard]] bool wake_observed(std::uint32_t observed) const noexcept;
    void spin_until_wake_or_timeout(std::uint32_t observed,
                                    std::chrono::nanoseconds timeout) noexcept;
    void yield_until_wake_or_timeout(std::uint32_t observed,
                                     std::chrono::nanoseconds timeout) noexcept;
    void wait_polling_until_wake_or_timeout(std::uint32_t observed,
                                            std::chrono::nanoseconds timeout,
                                            bool yield_wait) noexcept;
    static void idle_wait_once(bool yield_wait) noexcept;
    [[nodiscard]] bool prepare_wait(std::uint32_t &observed) noexcept;
    [[nodiscard]] bool run_due_timers() noexcept;
    void cancel_timers() noexcept;

    runtime &owner_;
    runtime_thread_info thread_;
    intrusive_mpsc_queue<runtime_work> inbox_;
    runtime_timer_heap timer_heap_;
    runtime_hierarchical_timer_wheel timer_wheel_;
    std::vector<runtime_service_task *> service_tasks_;
    std::unique_ptr<reactor> reactor_;
    std::size_t task_drain_budget_{256};
    std::chrono::nanoseconds max_task_run_slice_{0};
    std::size_t timer_drain_budget_{256};
    timer_kind timer_kind_{timer_kind::hierarchical_wheel};
    std::size_t service_task_budget_{32};
    std::size_t next_service_task_{0};
    std::uint64_t next_timer_sequence_{0};
    idle_wait_strategy idle_wait_{idle_wait_strategy::futex};
    wake_policy wake_policy_{wake_policy::empty_to_non_empty};
    cache_line_atomic<std::size_t> queued_work_count_{0};
    cache_line_atomic<std::uint32_t> wake_epoch_{0};
    cache_line_atomic<bool> stop_requested_{false};
    std::thread worker_;
};

} // namespace detail

} // namespace af
