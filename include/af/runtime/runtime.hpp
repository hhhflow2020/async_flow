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
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/runtime/atomic_wait.hpp"
#include "af/detail/runtime/cpu_relax.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/detail/thread/thread_attributes.hpp"
#include "af/detail/thread/thread_name.hpp"
#include "af/parallel.hpp"
#include "af/runtime/config_resolution.hpp"
#include "af/runtime/reactor.hpp"
#include "af/runtime/task.hpp"

namespace af {

class AsyncLogHandle;

namespace detail {

template <typename Fn> class runtime_delayed_function_task final : public runtime_task {
public:
    static_assert(std::is_invocable_v<Fn &, runtime &> || std::is_invocable_v<Fn &>,
                  "af::runtime::schedule_after callable must be invocable as fn(runtime&) or "
                  "fn()");

    runtime_delayed_function_task(runtime_task::factory_token token, runtime &owner, Fn &&fn)
        : runtime_task(token, owner), fn_(std::move(fn)) {}

    runtime_delayed_function_task(runtime_task::factory_token token, runtime &owner, const Fn &fn)
        : runtime_task(token, owner), fn_(fn) {}

    template <typename Rep, typename Period>
    [[nodiscard]] bool do_after(std::uint16_t thread,
                                std::chrono::duration<Rep, Period> delay) noexcept {
        return schedule_after(thread, delay);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool do_at(std::uint16_t thread,
                             std::chrono::time_point<Clock, Duration> time) noexcept {
        return schedule_at(thread, time);
    }

private:
    task_result run_task() noexcept override {
        try {
            if constexpr (std::is_invocable_v<Fn &, runtime &>) {
                fn_(owner());
            } else {
                fn_();
            }
        } catch (...) {
        }
        return done();
    }

    Fn fn_;
};

} // namespace detail

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

    [[nodiscard]] bool valid_thread(thread_ref thread) const noexcept {
        return resolution_.resolved.valid_thread(thread);
    }

    [[nodiscard]] af::thread_kind thread_kind_of(thread_index index) const noexcept {
        return resolution_.resolved.thread_kind_of(index);
    }

    [[nodiscard]] af::thread_kind thread_kind_of(thread_ref thread) const noexcept {
        return resolution_.resolved.thread_kind_of(thread);
    }

    [[nodiscard]] std::string_view thread_name(thread_index index) const noexcept {
        return resolution_.resolved.thread_name(index);
    }

    [[nodiscard]] std::string_view thread_name(thread_ref thread) const noexcept {
        return resolution_.resolved.thread_name(thread);
    }

    [[nodiscard]] thread_index thread_group_offset(thread_index index) const noexcept {
        return resolution_.resolved.thread_group_offset(index);
    }

    [[nodiscard]] thread_index thread_group_offset(thread_ref thread) const noexcept {
        return resolution_.resolved.thread_group_offset(thread);
    }

    [[nodiscard]] thread_index select_thread(thread_selector selector) const noexcept {
        return resolution_.resolved.select_thread(selector);
    }

    [[nodiscard]] thread_ref select_thread_ref(thread_selector selector) const noexcept {
        return resolution_.resolved.select_thread_ref(selector);
    }

    [[nodiscard]] thread_group_ref io_threads() const noexcept {
        return resolution_.resolved.io_thread_group();
    }

    [[nodiscard]] thread_group_ref cpu_threads() const noexcept {
        return resolution_.resolved.cpu_thread_group();
    }

    [[nodiscard]] thread_group_ref thread_group(std::size_t group_index) const noexcept {
        return resolution_.resolved.thread_group(group_index);
    }

    [[nodiscard]] thread_group_ref thread_group(std::string_view name) const noexcept {
        return resolution_.resolved.thread_group(name);
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
            ordered_batch_state_.assign(resolution_.resolved.threads.size(), ordered_batch_state{});
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
            ordered_batch_state_.clear();
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

    [[nodiscard]] bool post(thread_ref thread, runtime_work *work) noexcept {
        return post(thread.index, work);
    }

    template <typename Fn,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool post(thread_index thread, Fn &&fn) {
        if (!valid_thread(thread) || !running()) [[unlikely]] {
            return false;
        }

        using work_type = detail::runtime_function_work<std::decay_t<Fn>>;
        work_type *work = nullptr;
        try {
            work = new work_type(std::forward<Fn>(fn));
        } catch (...) {
            return false;
        }
        if (post(thread, static_cast<runtime_work *>(work))) [[likely]] {
            return true;
        }
        delete work;
        return false;
    }

    template <typename Fn,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool post(thread_ref thread, Fn &&fn) {
        return post(thread.index, std::forward<Fn>(fn));
    }

    template <typename Fn, typename Rep, typename Period,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool schedule_after(thread_index thread, std::chrono::duration<Rep, Period> delay,
                                      Fn &&fn) {
        if (!valid_thread(thread) || !running()) [[unlikely]] {
            return false;
        }

        using task_type = detail::runtime_delayed_function_task<std::decay_t<Fn>>;
        auto task = try_make_task<task_type>(*this, std::forward<Fn>(fn));
        return task && task->do_after(thread, delay);
    }

    template <typename Fn, typename Rep, typename Period,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool schedule_after(thread_ref thread, std::chrono::duration<Rep, Period> delay,
                                      Fn &&fn) {
        return schedule_after(thread.index, delay, std::forward<Fn>(fn));
    }

    template <typename Fn, typename Clock, typename Duration,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool schedule_at(thread_index thread,
                                   std::chrono::time_point<Clock, Duration> time, Fn &&fn) {
        if (!valid_thread(thread) || !running()) [[unlikely]] {
            return false;
        }

        using task_type = detail::runtime_delayed_function_task<std::decay_t<Fn>>;
        auto task = try_make_task<task_type>(*this, std::forward<Fn>(fn));
        return task && task->do_at(thread, time);
    }

    template <typename Fn, typename Clock, typename Duration,
              typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<Fn>, runtime_work *>>>
    [[nodiscard]] bool schedule_at(thread_ref thread, std::chrono::time_point<Clock, Duration> time,
                                   Fn &&fn) {
        return schedule_at(thread.index, time, std::forward<Fn>(fn));
    }

    template <typename Op, typename KeyFn>
    [[nodiscard]] static ShardedOps<Op> split_by_shard(std::vector<Op> &&ops,
                                                       std::uint16_t shard_count, KeyFn &&key_fn);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards(thread_group_ref shard_threads, ShardedOps<Op> &sharded_ops,
                                       parallel_mode mode, runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                                       parallel_mode mode, runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_group_ref shard_threads,
                                               ShardedOps<Op> &sharded_ops, std::uint64_t batch_id,
                                               runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_group_ref shard_threads,
                                               ShardedOps<Op> &sharded_ops, std::uint64_t batch_id,
                                               ordered_batch_options options, runtime_task *owner,
                                               Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                                               std::uint64_t batch_id, runtime_task *owner,
                                               Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                                               std::uint64_t batch_id,
                                               ordered_batch_options options, runtime_task *owner,
                                               Handler &&handler);

    template <typename StreamTag, typename ApplyTaskT, typename Batch>
    [[nodiscard]] bool start_ordered_task(thread_ref sequencer_thread, Batch &&batch);

    [[nodiscard]] std::uint64_t ordered_last_applied_batch_id(thread_ref thread) const noexcept;

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
        ordered_batch_state_.clear();
        active_thread_count_.store(0, std::memory_order_release);
        state_.store(runtime_state::stopped, std::memory_order_release);
    }

private:
    using ordered_batch_state = detail::OrderedBatchState;

    enum class ordered_guard_decision : std::uint8_t {
        run,
        skip_already_applied,
        fail,
    };

    class executor {
    public:
        executor(runtime &owner, runtime_thread_info thread)
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
                TimerEntry entry{detail::runtime_task_access::timer_deadline_ns(task),
                                 next_timer_sequence_++, task};
                if (timer_kind_ == timer_kind::min_heap) {
                    timer_heap_.push_back(entry);
                    std::push_heap(timer_heap_.begin(), timer_heap_.end(), timer_entry_after);
                } else {
                    timer_wheel_.push(entry, steady_now_ns());
                }
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
            static_cast<void>(detail::set_current_thread_priority(thread_.priority));
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

        [[nodiscard]] static bool timer_entry_before(const TimerEntry &left,
                                                     const TimerEntry &right) noexcept {
            if (left.deadline_ns != right.deadline_ns) {
                return left.deadline_ns < right.deadline_ns;
            }
            return left.sequence < right.sequence;
        }

        [[nodiscard]] static std::int64_t steady_now_ns() noexcept {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }

        class HierarchicalTimerWheel {
        public:
            HierarchicalTimerWheel() = default;

            void configure(const timer_config &config, std::size_t drain_budget) {
                tick_ns_ =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(config.tick).count();
                if (tick_ns_ <= 0) {
                    tick_ns_ = 1;
                }
                slot_count_ = config.wheel_slots == 0 ? 1 : config.wheel_slots;
                slot_mask_ = is_power_of_two(slot_count_) ? slot_count_ - 1U : 0U;
                level1_span_ticks_ = saturating_square(static_cast<std::uint64_t>(slot_count_));
                level0_.resize(slot_count_);
                level1_.resize(slot_count_);
                overflow_.reserve(config.initial_reserve);
                due_buffer_.reserve(drain_budget);
            }

            void push(TimerEntry entry, std::int64_t now_ns) {
                refresh_current_tick(now_ns);
                const std::uint64_t deadline_tick = tick_for(entry.deadline_ns);
                const std::uint64_t distance =
                    deadline_tick > current_tick_ ? deadline_tick - current_tick_ : 0U;
                if (distance < static_cast<std::uint64_t>(slot_count_)) {
                    level0_[slot_index(deadline_tick)].push_back(entry);
                } else if (distance < level1_span_ticks_) {
                    level1_[level1_slot_index(deadline_tick)].push_back(entry);
                } else {
                    overflow_.push_back(entry);
                    std::push_heap(overflow_.begin(), overflow_.end(), timer_entry_after);
                }
                ++pending_count_;
                if (entry.deadline_ns < next_deadline_ns_) {
                    next_deadline_ns_ = entry.deadline_ns;
                }
            }

            [[nodiscard]] std::chrono::nanoseconds
            wait_duration(std::int64_t now_ns) const noexcept {
                if (pending_count_ == 0) {
                    return std::chrono::nanoseconds::max();
                }
                if (next_deadline_ns_ <= now_ns) {
                    return std::chrono::nanoseconds(0);
                }
                return std::chrono::nanoseconds(next_deadline_ns_ - now_ns);
            }

            [[nodiscard]] bool run_due(std::int64_t now_ns, std::size_t budget,
                                       runtime &owner) noexcept {
                if (pending_count_ == 0 || budget == 0 || next_deadline_ns_ > now_ns) {
                    return false;
                }

                refresh_current_tick(now_ns);
                due_buffer_.clear();
                const std::uint64_t next_tick = tick_for(next_deadline_ns_);
                collect_due_from_bucket(level0_[slot_index(next_tick)], now_ns, budget,
                                        due_buffer_);
                if (due_buffer_.size() < budget) {
                    collect_due_from_bucket(level1_[level1_slot_index(next_tick)], now_ns, budget,
                                            due_buffer_);
                }
                if (due_buffer_.size() < budget) {
                    collect_due_from_overflow(now_ns, budget, due_buffer_);
                }
                if (due_buffer_.empty() && next_deadline_ns_ <= now_ns) [[unlikely]] {
                    collect_due_from_all(now_ns, budget, due_buffer_);
                }

                if (due_buffer_.empty()) [[unlikely]] {
                    rebuild_next_deadline();
                    return false;
                }

                if (due_buffer_.size() > 1U) {
                    std::sort(due_buffer_.begin(), due_buffer_.end(), timer_entry_before);
                }
                pending_count_ -= due_buffer_.size();
                bool did_work = false;
                for (TimerEntry &entry : due_buffer_) {
                    runtime_task *task = entry.task;
                    if (task == nullptr || !detail::runtime_task_access::mark_timer_ready(task)) {
                        continue;
                    }
                    did_work = true;
                    static_cast<runtime_work *>(task)->run(owner);
                }
                rebuild_next_deadline();
                return did_work;
            }

            void cancel_all() noexcept {
                for (auto &bucket : level0_) {
                    cancel_bucket(bucket);
                    bucket.clear();
                }
                for (auto &bucket : level1_) {
                    cancel_bucket(bucket);
                    bucket.clear();
                }
                cancel_bucket(overflow_);
                overflow_.clear();
                due_buffer_.clear();
                pending_count_ = 0;
                next_deadline_ns_ = std::numeric_limits<std::int64_t>::max();
            }

        private:
            [[nodiscard]] static bool is_power_of_two(std::size_t value) noexcept {
                return value != 0 && (value & (value - 1U)) == 0;
            }

            [[nodiscard]] static std::uint64_t saturating_square(std::uint64_t value) noexcept {
                if (value != 0 && value > std::numeric_limits<std::uint64_t>::max() / value) {
                    return std::numeric_limits<std::uint64_t>::max();
                }
                return value * value;
            }

            [[nodiscard]] std::uint64_t tick_for(std::int64_t deadline_ns) const noexcept {
                if (deadline_ns <= 0) {
                    return 0;
                }
                return static_cast<std::uint64_t>(deadline_ns) /
                       static_cast<std::uint64_t>(tick_ns_);
            }

            [[nodiscard]] std::size_t slot_index(std::uint64_t tick) const noexcept {
                if (slot_mask_ != 0U) {
                    return static_cast<std::size_t>(tick & static_cast<std::uint64_t>(slot_mask_));
                }
                return static_cast<std::size_t>(tick % static_cast<std::uint64_t>(slot_count_));
            }

            [[nodiscard]] std::size_t level1_slot_index(std::uint64_t tick) const noexcept {
                return slot_index(tick / static_cast<std::uint64_t>(slot_count_));
            }

            void refresh_current_tick(std::int64_t now_ns) noexcept {
                const std::uint64_t now_tick = tick_for(now_ns);
                if (!initialized_ || now_tick > current_tick_) {
                    current_tick_ = now_tick;
                    initialized_ = true;
                }
            }

            static void collect_due_from_bucket(std::vector<TimerEntry> &bucket,
                                                std::int64_t now_ns, std::size_t budget,
                                                std::vector<TimerEntry> &out) noexcept {
                if (bucket.empty() || out.size() >= budget) {
                    return;
                }
                std::size_t write = 0;
                for (std::size_t read = 0; read < bucket.size(); ++read) {
                    TimerEntry &entry = bucket[read];
                    if (entry.deadline_ns <= now_ns && out.size() < budget) {
                        out.push_back(entry);
                        continue;
                    }
                    if (write != read) {
                        bucket[write] = entry;
                    }
                    ++write;
                }
                bucket.resize(write);
            }

            static void cancel_bucket(const std::vector<TimerEntry> &bucket) noexcept {
                for (const TimerEntry &entry : bucket) {
                    detail::runtime_task_access::cancel_timer(entry.task);
                }
            }

            void collect_due_from_overflow(std::int64_t now_ns, std::size_t budget,
                                           std::vector<TimerEntry> &out) noexcept {
                while (out.size() < budget && !overflow_.empty() &&
                       overflow_.front().deadline_ns <= now_ns) {
                    std::pop_heap(overflow_.begin(), overflow_.end(), timer_entry_after);
                    out.push_back(overflow_.back());
                    overflow_.pop_back();
                }
            }

            void collect_due_from_all(std::int64_t now_ns, std::size_t budget,
                                      std::vector<TimerEntry> &out) noexcept {
                for (auto &bucket : level0_) {
                    collect_due_from_bucket(bucket, now_ns, budget, out);
                    if (out.size() >= budget) {
                        return;
                    }
                }
                for (auto &bucket : level1_) {
                    collect_due_from_bucket(bucket, now_ns, budget, out);
                    if (out.size() >= budget) {
                        return;
                    }
                }
                collect_due_from_overflow(now_ns, budget, out);
            }

            void rebuild_next_deadline() noexcept {
                std::int64_t next = std::numeric_limits<std::int64_t>::max();
                for (const auto &bucket : level0_) {
                    for (const TimerEntry &entry : bucket) {
                        if (entry.deadline_ns < next) {
                            next = entry.deadline_ns;
                        }
                    }
                }
                for (const auto &bucket : level1_) {
                    for (const TimerEntry &entry : bucket) {
                        if (entry.deadline_ns < next) {
                            next = entry.deadline_ns;
                        }
                    }
                }
                for (const TimerEntry &entry : overflow_) {
                    if (entry.deadline_ns < next) {
                        next = entry.deadline_ns;
                    }
                }
                next_deadline_ns_ =
                    pending_count_ == 0 ? std::numeric_limits<std::int64_t>::max() : next;
            }

            std::int64_t tick_ns_{1};
            std::size_t slot_count_{1};
            std::size_t slot_mask_{0};
            std::uint64_t level1_span_ticks_{1};
            std::uint64_t current_tick_{0};
            bool initialized_{false};
            std::size_t pending_count_{0};
            std::int64_t next_deadline_ns_{std::numeric_limits<std::int64_t>::max()};
            std::vector<std::vector<TimerEntry>> level0_;
            std::vector<std::vector<TimerEntry>> level1_;
            std::vector<TimerEntry> overflow_;
            std::vector<TimerEntry> due_buffer_;
        };

        [[nodiscard]] std::chrono::nanoseconds timer_wait_duration() const noexcept {
            if (timer_kind_ == timer_kind::hierarchical_wheel) {
                return timer_wheel_.wait_duration(steady_now_ns());
            }
            if (timer_heap_.empty()) {
                return std::chrono::nanoseconds::max();
            }

            const std::int64_t now = steady_now_ns();
            const std::int64_t deadline = timer_heap_.front().deadline_ns;
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
            if (timer_kind_ == timer_kind::hierarchical_wheel) {
                return timer_wheel_.run_due(steady_now_ns(), timer_drain_budget_, owner_);
            }
            bool did_work = false;
            std::size_t drained = 0;
            while (drained < timer_drain_budget_ && !timer_heap_.empty()) {
                const std::int64_t now = steady_now_ns();
                if (timer_heap_.front().deadline_ns > now) {
                    break;
                }

                std::pop_heap(timer_heap_.begin(), timer_heap_.end(), timer_entry_after);
                TimerEntry entry = timer_heap_.back();
                timer_heap_.pop_back();
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
            if (timer_kind_ == timer_kind::hierarchical_wheel) {
                timer_wheel_.cancel_all();
                return;
            }
            for (TimerEntry &entry : timer_heap_) {
                detail::runtime_task_access::cancel_timer(entry.task);
            }
            timer_heap_.clear();
        }

        runtime &owner_;
        runtime_thread_info thread_;
        detail::IntrusiveMpscQueue<runtime_work> inbox_;
        std::vector<TimerEntry> timer_heap_;
        HierarchicalTimerWheel timer_wheel_;
        std::vector<detail::RuntimeServiceTask *> service_tasks_;
        std::unique_ptr<reactor> reactor_;
        std::size_t task_drain_budget_{256};
        std::chrono::nanoseconds max_task_run_slice_{0};
        std::size_t timer_drain_budget_{256};
        timer_kind timer_kind_{timer_kind::min_heap};
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

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] bool
    parallel_shards_impl(std::bool_constant<Ordered>, thread_group_ref shard_threads,
                         ShardedOps<Op> &sharded_ops, parallel_mode mode, std::uint64_t batch_id,
                         ordered_batch_options options, runtime_task *owner, Handler &&handler);

    [[nodiscard]] ordered_guard_decision check_order_guard(std::uint64_t batch_id,
                                                           ordered_batch_options options) noexcept;

    void commit_order_guard(std::uint64_t batch_id) noexcept;

    template <typename StreamTag, typename ApplyTaskT, typename BatchT> struct ordered_start_state;

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    [[nodiscard]] ordered_start_state<StreamTag, ApplyTaskT, BatchT> &
    ordered_start_state_for_thread();

    template <typename StreamTag, typename ApplyTaskT, typename BatchT> class ordered_start_task;

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] bool run_parallel_shard(std::uint16_t shard_index, std::uint64_t batch_id,
                                          ordered_batch_options options, std::vector<Op> &ops,
                                          Handler &handler) noexcept;

    template <typename Op, typename Handler, bool Ordered> class parallel_shard_task;

    runtime_config_resolution resolution_;
    std::vector<std::unique_ptr<executor>> executors_;
    std::vector<ordered_batch_state> ordered_batch_state_;
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
    owned_logger_->stop(
        std::chrono::duration_cast<std::chrono::milliseconds>(config().shutdown.log_flush_timeout));
    owned_logger_.reset();
}

} // namespace af

#include "af/runtime/parallel.hpp"
#include "af/runtime/task_impl.hpp"
