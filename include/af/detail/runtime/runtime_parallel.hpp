#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_parallel.hpp must be included by async_runtime.hpp"
#endif

namespace af {

template <typename TraitsT>
template <typename Op, typename Handler, bool Ordered>
void AsyncRuntime<TraitsT>::parallel_shards_impl(std::bool_constant<Ordered>, Thread shard_begin,
                                                 ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                                 std::uint64_t batch_id,
                                                 OrderedBatchOptions ordered_options, Task *owner,
                                                 Handler &&handler) {
    AF_ASSERT(owner != nullptr);
    AF_ASSERT(is_runtime_thread() && "parallel_shards must be called from a runtime thread");

    const std::uint16_t begin = thread_index(shard_begin);
    const std::uint16_t shard_count = sharded_ops.shard_count();
    AF_ASSERT(begin + shard_count <= thread_count);

    std::uint32_t target_count = shard_count;
    if (mode == ParallelMode::NonEmptyOnly) {
        target_count = 0;
        for (std::uint16_t i = 0; i < shard_count; ++i) {
            if (!sharded_ops.shards[i].empty()) {
                ++target_count;
            }
        }
    }

    if (target_count == 0) {
        post_blocking(current_thread(), owner);
        return;
    }

    auto *group = create_parallel_group(target_count, owner, current_thread_index());

    using HandlerT = std::decay_t<Handler>;
    for (std::uint16_t i = 0; i < shard_count; ++i) {
        if (mode == ParallelMode::NonEmptyOnly && sharded_ops.shards[i].empty()) {
            continue;
        }

        const Thread thread = thread_from_index(static_cast<std::uint16_t>(begin + i));
        if (thread_index(thread) == current_thread_index()) {
            auto ops = std::move(sharded_ops.shards[i]);
            HandlerT local_handler(handler);
            const bool ok = run_parallel_shard<Op, HandlerT, Ordered>(i, batch_id, ordered_options,
                                                                      ops, local_handler);
            group->complete(ok);
            continue;
        }

        Task *shard_task = nullptr;
        if constexpr (Ordered) {
            shard_task = allocate_task<ShardTask<Op, HandlerT, true>>(
                group, i, batch_id, ordered_options, std::move(sharded_ops.shards[i]),
                HandlerT(handler));
        } else {
            shard_task = allocate_task<ShardTask<Op, HandlerT, false>>(
                group, i, 0, OrderedBatchOptions{}, std::move(sharded_ops.shards[i]),
                HandlerT(handler));
        }

        post_blocking(thread, shard_task);
    }
}

template <typename TraitsT>
auto AsyncRuntime<TraitsT>::check_order_guard(std::uint64_t batch_id,
                                              OrderedBatchOptions options) noexcept
    -> OrderedGuardDecision {
    const std::uint16_t thread = AsyncRuntime::current_thread_index();
    AF_ASSERT(thread < ordered_batch_state_.size());
    auto &state = ordered_batch_state_[thread];
    if (batch_id == state.last_applied_batch_id + 1U) {
        return OrderedGuardDecision::Run;
    }
    if (options.replay_policy == OrderedBatchReplayPolicy::SkipAlreadyApplied &&
        batch_id == state.last_applied_batch_id) {
        return OrderedGuardDecision::SkipAlreadyApplied;
    }
    const bool ok = false;
    AF_ASSERT(ok && "ordered batch id must be contiguous per shard");
    static_cast<void>(ok);
    return OrderedGuardDecision::Fail;
}

template <typename TraitsT>
void AsyncRuntime<TraitsT>::commit_order_guard(std::uint64_t batch_id) noexcept {
    const std::uint16_t thread = AsyncRuntime::current_thread_index();
    ordered_batch_state_[thread].last_applied_batch_id = batch_id;
}

template <typename TraitsT>
template <typename StreamTag, typename ApplyTaskT, typename BatchT>
struct AsyncRuntime<TraitsT>::OrderedStartState {
    std::uint64_t next_batch_id{1};
    std::uint64_t generation{0};
    absl::flat_hash_map<std::uint64_t, BatchT> pending;

    void reset(std::uint64_t runtime_generation) {
        next_batch_id = 1;
        generation = runtime_generation;
        pending.clear();
    }

    [[nodiscard]] bool submit(BatchT batch) {
        const std::uint64_t batch_id = batch.batch_id;
        if (batch_id < next_batch_id) {
            return true;
        }

        if (batch_id > next_batch_id) {
            pending.emplace(batch_id, std::move(batch));
            return true;
        }

        if (!start_ready(std::move(batch))) {
            return false;
        }
        return drain_ready();
    }

    [[nodiscard]] bool start_ready(BatchT batch) {
        const bool ok = AsyncRuntime::template start_task<ApplyTaskT>(std::move(batch));
        if (!ok) {
            return false;
        }
        ++next_batch_id;
        return true;
    }

    [[nodiscard]] bool drain_ready() {
        for (;;) {
            auto it = pending.find(next_batch_id);
            if (it == pending.end()) {
                return true;
            }

            BatchT batch = std::move(it->second);
            pending.erase(it);
            if (!start_ready(std::move(batch))) {
                return false;
            }
        }
    }
};

template <typename TraitsT>
template <typename StreamTag, typename ApplyTaskT, typename BatchT>
auto AsyncRuntime<TraitsT>::ordered_start_state() ->
    typename AsyncRuntime<TraitsT>::template OrderedStartState<StreamTag, ApplyTaskT, BatchT> & {
    static std::vector<OrderedStartState<StreamTag, ApplyTaskT, BatchT>> states(thread_count);
    const std::uint16_t index = current_thread_index();
    AF_ASSERT(index < states.size());
    auto &state = states[index];
    const std::uint64_t runtime_generation = generation_.load(std::memory_order_acquire);
    if (state.generation != runtime_generation) {
        state.reset(runtime_generation);
    }
    return state;
}

template <typename TraitsT>
template <typename StreamTag, typename ApplyTaskT, typename BatchT>
class AsyncRuntime<TraitsT>::OrderedStartTask final : public AsyncRuntime<TraitsT>::Task {
public:
    using RuntimeTask = typename AsyncRuntime<TraitsT>::Task;

    explicit OrderedStartTask(typename RuntimeTask::FactoryToken token) : RuntimeTask(token) {}

    bool do_it(Thread sequencer_thread, BatchT batch) {
        batch_ = std::move(batch);
        return this->schedule(sequencer_thread);
    }

private:
    TaskResult run() override {
        const bool ok =
            ordered_start_state<StreamTag, ApplyTaskT, BatchT>().submit(std::move(batch_));
        return ok ? this->done() : this->failed();
    }

    BatchT batch_{};
};

template <typename TraitsT>
template <typename Op, typename Handler, bool Ordered>
bool AsyncRuntime<TraitsT>::run_parallel_shard(std::uint16_t shard_index, std::uint64_t batch_id,
                                               OrderedBatchOptions options, std::vector<Op> &ops,
                                               Handler &handler) noexcept {
    bool ok = true;
    bool skip_handler = false;
    if constexpr (Ordered) {
        const OrderedGuardDecision decision = check_order_guard(batch_id, options);
        ok = decision != OrderedGuardDecision::Fail;
        skip_handler = decision == OrderedGuardDecision::SkipAlreadyApplied;
    }

    if (ok && !skip_handler) {
        try {
            if constexpr (Ordered) {
                using HandlerResult = std::invoke_result_t<Handler &, std::uint16_t,
                                                           std::vector<Op> &, std::uint64_t>;
                if constexpr (std::is_same_v<HandlerResult, bool>) {
                    ok = handler(shard_index, ops, batch_id);
                } else {
                    handler(shard_index, ops, batch_id);
                }
            } else {
                using HandlerResult =
                    std::invoke_result_t<Handler &, std::uint16_t, std::vector<Op> &>;
                if constexpr (std::is_same_v<HandlerResult, bool>) {
                    ok = handler(shard_index, ops);
                } else {
                    handler(shard_index, ops);
                }
            }
        } catch (...) {
            AF_ASSERT(false && "parallel shard handler must not throw");
            ok = false;
        }
    }

    if constexpr (Ordered) {
        if (ok && !skip_handler) {
            commit_order_guard(batch_id);
        }
    }
    return ok;
}

template <typename TraitsT>
template <typename Op, typename Handler, bool Ordered>
class AsyncRuntime<TraitsT>::ShardTask final : public AsyncRuntime<TraitsT>::Task {
public:
    using RuntimeTask = typename AsyncRuntime<TraitsT>::Task;

    ShardTask(typename RuntimeTask::FactoryToken token, ParallelGroup *group,
              std::uint16_t shard_index, std::uint64_t batch_id, OrderedBatchOptions options,
              std::vector<Op> &&ops, Handler handler)
        : RuntimeTask(token), group_(group), shard_index_(shard_index), batch_id_(batch_id),
          options_(options), ops_(std::move(ops)), handler_(std::move(handler)) {}

private:
    TaskResult run() override {
        const bool ok = run_parallel_shard<Op, Handler, Ordered>(shard_index_, batch_id_, options_,
                                                                 ops_, handler_);
        group_->complete(ok);
        return this->done();
    }

    void on_runtime_cancel() noexcept override {
        group_->complete(false, false);
    }

    ParallelGroup *group_;
    std::uint16_t shard_index_;
    std::uint64_t batch_id_;
    OrderedBatchOptions options_;
    std::vector<Op> ops_;
    Handler handler_;
};

} // namespace af
