#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_public_api.hpp must be included by async_runtime.hpp"
#endif

namespace af {

template <typename TraitsT> void AsyncRuntime<TraitsT>::init() {
    RuntimeStatus expected = RuntimeStatus::Stopped;
    if (!status_.compare_exchange_strong(expected, RuntimeStatus::Starting,
                                         std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    ordered_batch_state_.assign(thread_count, OrderedBatchState{});
    generation_.fetch_add(1, std::memory_order_acq_rel);
    reset_task_registry();
    init_queues();
    executors_.clear();
    executors_.reserve(thread_count);
    for (std::uint16_t i = 0; i < thread_count; ++i) {
        executors_.push_back(std::make_unique<Executor>(i));
    }
    for (auto &executor : executors_) {
        executor->start();
    }
    status_.store(RuntimeStatus::Running, std::memory_order_release);
}

template <typename TraitsT> void AsyncRuntime<TraitsT>::shutdown() {
    AF_ASSERT(!is_runtime_thread() && "shutdown must be called from a non-runtime thread");
    if (is_runtime_thread()) {
        return;
    }

    RuntimeStatus expected = RuntimeStatus::Running;
    if (!status_.compare_exchange_strong(expected, RuntimeStatus::Stopping,
                                         std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    wait_for_external_posts();
    if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
        wait_for_idle();
    }

    for (auto &executor : executors_) {
        executor->request_stop();
    }
    for (auto &executor : executors_) {
        executor->join();
    }
    if constexpr (shutdown_policy == ShutdownPolicy::StopImmediately) {
        cancel_registered_tasks();
    } else {
        reset_task_registry();
    }

    executors_.clear();
    spsc_queues_.clear();
    external_queues_.clear();
    ordered_batch_state_.clear();
    unfinished_tasks_.store(0, std::memory_order_release);
    unfinished_tasks_.notify_all();
    status_.store(RuntimeStatus::Stopped, std::memory_order_release);
}

template <typename TraitsT> void AsyncRuntime<TraitsT>::wait_for_idle() noexcept {
    for (;;) {
        const std::uint32_t count = unfinished_tasks_.load(std::memory_order_acquire);
        if (count == 0) {
            return;
        }
        unfinished_tasks_.wait(count, std::memory_order_acquire);
    }
}

template <typename TraitsT> std::uint32_t AsyncRuntime<TraitsT>::unfinished_task_count() noexcept {
    return unfinished_tasks_.load(std::memory_order_acquire);
}

template <typename TraitsT>
template <typename TaskT, typename... CtorArgs>
auto AsyncRuntime<TraitsT>::make_task(CtorArgs &&...ctor_args) ->
    typename AsyncRuntime<TraitsT>::template TaskHandle<TaskT> {
    static_assert(std::is_base_of_v<Task, TaskT>, "TaskT must derive from Runtime::Task");
    auto *task = allocate_task<TaskT>(std::forward<CtorArgs>(ctor_args)...);
    task->attach_start_handle();
    return TaskHandle<TaskT>(task);
}

template <typename TraitsT>
template <typename TaskT, typename... CtorArgs>
auto AsyncRuntime<TraitsT>::create_task(CtorArgs &&...ctor_args) ->
    typename AsyncRuntime<TraitsT>::template TaskHandle<TaskT> {
    return make_task<TaskT>(std::forward<CtorArgs>(ctor_args)...);
}

template <typename TraitsT>
template <typename TaskT, typename... Args>
bool AsyncRuntime<TraitsT>::start_task(Args &&...args) {
    static_assert(std::is_base_of_v<Task, TaskT>, "TaskT must derive from Runtime::Task");
    auto task = make_task<TaskT>();
    using DoItResult = decltype(task.get()->do_it(std::declval<Args>()...));

    if constexpr (std::is_void_v<DoItResult>) {
        task->do_it(std::forward<Args>(args)...);
        return task.scheduled();
    } else {
        const bool ok = static_cast<bool>(task->do_it(std::forward<Args>(args)...));
        return ok && task.scheduled();
    }
}

template <typename TraitsT> bool AsyncRuntime<TraitsT>::post(Thread thread, Task *task) noexcept {
    if (task == nullptr) {
        return false;
    }

    const std::uint16_t index = thread_index(thread);
    if (index >= thread_count) {
        AF_ASSERT(false && "invalid thread index");
        return false;
    }

    if (!try_enter_post(index)) {
        return false;
    }

    const detail::ScheduleRequest request = task->request_schedule(index);
    if (request.action == detail::ScheduleAction::Enqueue) {
        const bool first_schedule = request.previous == TaskState::Created;
        if (first_schedule) {
            on_task_started(task);
        }
        const bool enqueued = enqueue_ready_by_policy(index, task);
        if (!enqueued) {
            if (first_schedule) {
                on_task_finished(task);
            }
            task->cancel_schedule(request.previous);
        }
        leave_post(index);
        return enqueued;
    }
    const bool deferred = request.action == detail::ScheduleAction::Deferred;
    leave_post(index);
    return deferred;
}

template <typename TraitsT>
auto AsyncRuntime<TraitsT>::current_thread() noexcept -> typename AsyncRuntime<TraitsT>::Thread {
    return thread_from_index(current_thread_index_);
}

template <typename TraitsT> bool AsyncRuntime<TraitsT>::is_runtime_thread() noexcept {
    return current_thread_index_ < thread_count;
}

template <typename TraitsT> bool AsyncRuntime<TraitsT>::is_stopping() noexcept {
    return status_.load(std::memory_order_acquire) == RuntimeStatus::Stopping;
}

template <typename TraitsT> std::uint16_t AsyncRuntime<TraitsT>::current_thread_index() noexcept {
    return current_thread_index_;
}

template <typename TraitsT>
template <typename Op, typename KeyFn>
ShardedOps<Op> AsyncRuntime<TraitsT>::split_by_shard(std::vector<Op> &&ops,
                                                     std::uint16_t shard_count, KeyFn &&key_fn) {
    AF_ASSERT(shard_count > 0);
    ShardedOps<Op> sharded(shard_count);
    if (shard_count == 0) {
        return sharded;
    }

    std::vector<std::size_t> shard_sizes(shard_count, 0);
    std::vector<std::uint16_t> shard_indexes;
    shard_indexes.reserve(ops.size());
    for (auto &op : ops) {
        const auto key = static_cast<std::uint64_t>(key_fn(op));
        const std::uint16_t shard = static_cast<std::uint16_t>(key % shard_count);
        shard_indexes.push_back(shard);
        ++shard_sizes[shard];
    }

    for (std::uint16_t shard = 0; shard < shard_count; ++shard) {
        sharded.shards[shard].reserve(shard_sizes[shard]);
    }

    std::size_t index = 0;
    for (auto &op : ops) {
        sharded.shards[shard_indexes[index++]].push_back(std::move(op));
    }
    return sharded;
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards(Thread shard_begin, ShardedOps<Op> &sharded_ops,
                                            ParallelMode mode, Task *owner, Handler &&handler) {
    parallel_shards_impl(std::bool_constant<false>{}, shard_begin, sharded_ops, mode, 0,
                         OrderedBatchOptions{}, owner, std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards(ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                            Task *owner, Handler &&handler) {
    parallel_shards(thread_from_index(0), sharded_ops, mode, owner, std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards_ordered(Thread shard_begin, ShardedOps<Op> &sharded_ops,
                                                    std::uint64_t batch_id, Task *owner,
                                                    Handler &&handler) {
    parallel_shards_impl(std::bool_constant<true>{}, shard_begin, sharded_ops,
                         ParallelMode::AllShards, batch_id, OrderedBatchOptions{}, owner,
                         std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards_ordered(Thread shard_begin, ShardedOps<Op> &sharded_ops,
                                                    std::uint64_t batch_id,
                                                    OrderedBatchOptions options, Task *owner,
                                                    Handler &&handler) {
    parallel_shards_impl(std::bool_constant<true>{}, shard_begin, sharded_ops,
                         ParallelMode::AllShards, batch_id, options, owner,
                         std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards_ordered(ShardedOps<Op> &sharded_ops,
                                                    std::uint64_t batch_id, Task *owner,
                                                    Handler &&handler) {
    parallel_shards_ordered(thread_from_index(0), sharded_ops, batch_id, owner,
                            std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards_ordered(ShardedOps<Op> &sharded_ops,
                                                    std::uint64_t batch_id,
                                                    OrderedBatchOptions options, Task *owner,
                                                    Handler &&handler) {
    parallel_shards_ordered(thread_from_index(0), sharded_ops, batch_id, options, owner,
                            std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename Op, typename Handler>
void AsyncRuntime<TraitsT>::parallel_shards(ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                            std::uint64_t batch_id, Task *owner,
                                            Handler &&handler) {
    AF_ASSERT(mode == ParallelMode::AllShards);
    static_cast<void>(mode);
    parallel_shards_ordered(thread_from_index(0), sharded_ops, batch_id, owner,
                            std::forward<Handler>(handler));
}

template <typename TraitsT>
template <typename StreamTag, typename ApplyTaskT, typename Batch>
bool AsyncRuntime<TraitsT>::start_ordered_task(Thread sequencer_thread, Batch &&batch) {
    using BatchT = std::decay_t<Batch>;
    auto *task = allocate_task<OrderedStartTask<StreamTag, ApplyTaskT, BatchT>>();
    const bool ok = task->do_it(sequencer_thread, std::forward<Batch>(batch));
    if (!ok && task->is_created()) {
        task->release_lifetime_ref();
    }
    return ok;
}

template <typename TraitsT>
std::uint64_t AsyncRuntime<TraitsT>::ordered_last_applied_batch_id(Thread thread) noexcept {
    const std::uint16_t index = thread_index(thread);
    if (index >= ordered_batch_state_.size()) {
        AF_ASSERT(false && "invalid ordered batch thread");
        return 0;
    }
    return ordered_batch_state_[index].last_applied_batch_id;
}

} // namespace af
