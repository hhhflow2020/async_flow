#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/runtime/atomic_wait.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/parallel.hpp"
#include "af/runtime/config_resolution.hpp"
#include "af/runtime/detail/executor.hpp"
#include "af/runtime/detail/function_task.hpp"
#include "af/runtime/detail/function_work.hpp"
#include "af/runtime/detail/pooled_object.hpp"
#include "af/runtime/reactor.hpp"
#include "af/runtime/task.hpp"

namespace af {

class async_log_handle;

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

    [[nodiscard]] bool register_reactor_source(thread_index thread,
                                               fd_event_source *source) noexcept;

    [[nodiscard]] bool register_reactor_source(thread_ref thread,
                                               fd_event_source *source) noexcept {
        return register_reactor_source(thread.index, source);
    }

    [[nodiscard]] bool update_reactor_source(thread_index thread, fd_event_source *source) noexcept;

    [[nodiscard]] bool update_reactor_source(thread_ref thread, fd_event_source *source) noexcept {
        return update_reactor_source(thread.index, source);
    }

    [[nodiscard]] bool unregister_reactor_source(thread_index thread,
                                                 fd_event_source *source) noexcept;

    [[nodiscard]] bool unregister_reactor_source(thread_ref thread,
                                                 fd_event_source *source) noexcept {
        return unregister_reactor_source(thread.index, source);
    }

    [[nodiscard]] bool logger_started() const noexcept {
        return owned_logger_ != nullptr;
    }

    [[nodiscard]] bool
    flush_logger(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept;

    [[nodiscard]] bool start();

    [[nodiscard]] bool post(thread_index thread, runtime_work *work) noexcept {
        if (work == nullptr || !valid_thread(thread)) {
            return false;
        }
        if (!try_enter_post()) {
            return false;
        }

        track_work_started();
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
        const task_pool_config &pool_config = config().task_pool;
        return detail::visit_runtime_pooled_object_pool_holder<work_type>(
            pool_config.local_cache_size, [&](auto &holder) -> bool {
                if (!holder.try_reserve_at_least(pool_config.slab_object_count)) {
                    return false;
                }
                work_type *work = nullptr;
                try {
                    work = holder.pool.create(
                        &detail::destroy_runtime_pooled_object<
                            work_type, std::decay_t<decltype(holder)>::local_cache_capacity>,
                        std::forward<Fn>(fn));
                } catch (...) {
                    return false;
                }
                if (post(thread, static_cast<runtime_work *>(work))) [[likely]] {
                    return true;
                }
                work->destroy();
                return false;
            });
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
    [[nodiscard]] static sharded_ops<Op> split_by_shard(std::vector<Op> &&ops,
                                                        std::uint16_t shard_count, KeyFn &&key_fn);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards(thread_group_ref shard_threads, sharded_ops<Op> &sharded_ops,
                                       parallel_mode mode, runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards(thread_ref shard_begin, sharded_ops<Op> &sharded_ops,
                                       parallel_mode mode, runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_group_ref shard_threads,
                                               sharded_ops<Op> &sharded_ops, std::uint64_t batch_id,
                                               runtime_task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_group_ref shard_threads,
                                               sharded_ops<Op> &sharded_ops, std::uint64_t batch_id,
                                               ordered_batch_options options, runtime_task *owner,
                                               Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_ref shard_begin, sharded_ops<Op> &sharded_ops,
                                               std::uint64_t batch_id, runtime_task *owner,
                                               Handler &&handler);

    template <typename Op, typename Handler>
    [[nodiscard]] bool parallel_shards_ordered(thread_ref shard_begin, sharded_ops<Op> &sharded_ops,
                                               std::uint64_t batch_id,
                                               ordered_batch_options options, runtime_task *owner,
                                               Handler &&handler);

    template <typename stream_tag_t, typename apply_task_t, typename batch_t>
    [[nodiscard]] bool start_ordered_task(thread_ref sequencer_thread, batch_t &&batch);

    [[nodiscard]] std::uint64_t ordered_last_applied_batch_id(thread_ref thread) const noexcept;

    [[nodiscard]] bool register_service_task(thread_index thread,
                                             detail::runtime_service_task *service) noexcept {
        if (service == nullptr || thread >= executors_.size()) {
            return false;
        }
        return executors_[thread]->register_service_task(service);
    }

    [[nodiscard]] bool unregister_service_task(thread_index thread,
                                               detail::runtime_service_task *service) noexcept {
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

    void stop() noexcept;

private:
    using ordered_batch_state = detail::ordered_batch_state;

    enum class ordered_guard_decision : std::uint8_t {
        run,
        skip_already_applied,
        fail,
    };

    [[nodiscard]] static std::string status_message(runtime_config_validation_result validation);

    void request_stop() noexcept;
    void join_all() noexcept;
    [[nodiscard]] bool has_active_work() const noexcept;
    void track_work_started() noexcept;
    void track_work_finished() noexcept;
    [[nodiscard]] bool can_post_from_stopping_runtime_thread() const noexcept;
    [[nodiscard]] bool try_enter_post() noexcept;
    void leave_post() noexcept;
    void wait_for_posts() noexcept;
    void on_executor_started() noexcept;
    void on_executor_stopped() noexcept;
    void arm_timer_on_current_executor(runtime_task *task) noexcept;
    [[nodiscard]] static task_id_type exchange_current_task_id(task_id_type next) noexcept;

    void start_owned_logger_if_configured();
    void stop_owned_logger() noexcept;

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] bool
    parallel_shards_impl(std::bool_constant<Ordered>, thread_group_ref shard_threads,
                         sharded_ops<Op> &sharded_ops, parallel_mode mode, std::uint64_t batch_id,
                         ordered_batch_options options, runtime_task *owner, Handler &&handler);

    [[nodiscard]] ordered_guard_decision check_order_guard(std::uint64_t batch_id,
                                                           ordered_batch_options options) noexcept;

    void commit_order_guard(std::uint64_t batch_id) noexcept;

    template <typename stream_tag_t, typename apply_task_t, typename batch_t>
    struct ordered_start_state;

    template <typename stream_tag_t, typename apply_task_t, typename batch_t>
    [[nodiscard]] ordered_start_state<stream_tag_t, apply_task_t, batch_t> &
    ordered_start_state_for_thread();

    template <typename stream_tag_t, typename apply_task_t, typename batch_t>
    class ordered_start_task;

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] bool run_parallel_shard(std::uint16_t shard_index, std::uint64_t batch_id,
                                          ordered_batch_options options, std::vector<Op> &ops,
                                          Handler &handler) noexcept;

    template <typename Op, typename Handler, bool Ordered> class parallel_shard_task;

    runtime_config_resolution resolution_;
    std::vector<std::unique_ptr<detail::runtime_executor>> executors_;
    std::vector<ordered_batch_state> ordered_batch_state_;
    std::unique_ptr<async_log_handle> owned_logger_;
    std::atomic<runtime_state> state_{runtime_state::stopped};
    std::atomic<bool> owned_logger_stop_started_{false};
    std::atomic<thread_index> active_thread_count_{0};
    std::atomic<std::uint32_t> posting_count_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::uint32_t> active_work_count_{0};
    std::atomic<std::uint32_t> active_epoch_{0};

    inline static thread_local runtime *current_runtime_{nullptr};
    inline static thread_local detail::runtime_executor *current_executor_{nullptr};
    inline static thread_local thread_index current_thread_index_{runtime_invalid_thread_index};
    inline static thread_local task_id_type current_task_id_{invalid_task_id};

    friend class runtime_task;
    friend class detail::runtime_executor;
};

} // namespace af

#include "af/runtime/detail/executor_impl.hpp"
#include "af/runtime/detail/runtime_lifecycle.hpp"
#include "af/runtime/parallel.hpp"
#include "af/runtime/task_impl.hpp"
