#pragma once

#include <type_traits>

#include "absl/container/flat_hash_map.h"

namespace af {

namespace detail {

struct RuntimeInstanceParallelGroup;

void destroy_runtime_instance_parallel_group(RuntimeInstanceParallelGroup *group) noexcept;

struct RuntimeInstanceParallelGroup {
    std::atomic<std::uint32_t> pending{0};
    runtime *owner{nullptr};
    runtime_task *owner_task{nullptr};
    std::uint16_t resume_thread{runtime_invalid_thread_index};
    std::atomic<std::uint32_t> failed{0};

    void init(runtime &runtime_owner, std::uint32_t target_count, runtime_task *task,
              std::uint16_t target_thread) noexcept {
        pending.store(target_count, std::memory_order_relaxed);
        owner = &runtime_owner;
        owner_task = task;
        resume_thread = target_thread;
        failed.store(0, std::memory_order_relaxed);
        runtime_task_access::add_lifetime_ref(owner_task);
    }

    void complete(bool ok, bool resume_owner = true) noexcept {
        if (!ok) {
            failed.fetch_add(1, std::memory_order_relaxed);
        }
        if (pending.fetch_sub(1, std::memory_order_acq_rel) != 1U) {
            return;
        }

        runtime *runtime_owner = owner;
        runtime_task *task = owner_task;
        if (resume_owner && runtime_owner != nullptr && task != nullptr &&
            runtime_owner->valid_thread(resume_thread)) {
            runtime_task_access::set_last_parallel_failures(task,
                                                            failed.load(std::memory_order_acquire));
            static_cast<void>(runtime_task_access::schedule_to(task, resume_thread));
        }
        runtime_task_access::release_lifetime_ref(task);
        destroy_runtime_instance_parallel_group(this);
    }
};

using RuntimeInstanceParallelGroupPool =
    ObjectPool<RuntimeInstanceParallelGroup, 4096, 64, false, 1, 4, 256>;

[[nodiscard]] inline RuntimeInstanceParallelGroupPool &runtime_instance_parallel_group_pool() {
    static RuntimeInstanceParallelGroupPool pool;
    return pool;
}

[[nodiscard]] inline RuntimeInstanceParallelGroup *
create_runtime_instance_parallel_group(runtime &owner, std::uint32_t target_count,
                                       runtime_task *task, std::uint16_t resume_thread) {
    RuntimeInstanceParallelGroup *group = runtime_instance_parallel_group_pool().create();
    group->init(owner, target_count, task, resume_thread);
    return group;
}

inline void destroy_runtime_instance_parallel_group(RuntimeInstanceParallelGroup *group) noexcept {
    runtime_instance_parallel_group_pool().destroy(group);
}

} // namespace detail

template <typename Op, typename KeyFn>
ShardedOps<Op> runtime::split_by_shard(std::vector<Op> &&ops, std::uint16_t shard_count,
                                       KeyFn &&key_fn) {
    AF_ASSERT(shard_count > 0);
    ShardedOps<Op> sharded(shard_count);
    if (shard_count == 0) {
        return sharded;
    }

    for (auto &op : ops) {
        const auto shard_value = static_cast<std::uint64_t>(key_fn(op));
        const auto shard = static_cast<std::uint16_t>(shard_value % shard_count);
        sharded.shards[shard].push_back(std::move(op));
    }
    return sharded;
}

template <typename Op, typename Handler>
bool runtime::parallel_shards(thread_group_ref shard_threads, ShardedOps<Op> &sharded_ops,
                              parallel_mode mode, runtime_task *owner, Handler &&handler) {
    return parallel_shards_impl(std::bool_constant<false>{}, shard_threads, sharded_ops, mode, 0,
                                ordered_batch_options{}, owner, std::forward<Handler>(handler));
}

template <typename Op, typename Handler>
bool runtime::parallel_shards(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                              parallel_mode mode, runtime_task *owner, Handler &&handler) {
    std::vector<std::uint16_t> threads;
    threads.reserve(sharded_ops.shard_count());
    for (std::uint16_t i = 0; i < sharded_ops.shard_count(); ++i) {
        threads.push_back(static_cast<std::uint16_t>(shard_begin.index + i));
    }
    return parallel_shards(thread_group_ref(threads.data(), threads.size()), sharded_ops, mode,
                           owner, std::forward<Handler>(handler));
}

template <typename Op, typename Handler>
bool runtime::parallel_shards_ordered(thread_group_ref shard_threads, ShardedOps<Op> &sharded_ops,
                                      std::uint64_t batch_id, runtime_task *owner,
                                      Handler &&handler) {
    return parallel_shards_ordered(shard_threads, sharded_ops, batch_id, ordered_batch_options{},
                                   owner, std::forward<Handler>(handler));
}

template <typename Op, typename Handler>
bool runtime::parallel_shards_ordered(thread_group_ref shard_threads, ShardedOps<Op> &sharded_ops,
                                      std::uint64_t batch_id, ordered_batch_options options,
                                      runtime_task *owner, Handler &&handler) {
    return parallel_shards_impl(std::bool_constant<true>{}, shard_threads, sharded_ops,
                                parallel_mode::all_shards, batch_id, options, owner,
                                std::forward<Handler>(handler));
}

template <typename Op, typename Handler>
bool runtime::parallel_shards_ordered(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                                      std::uint64_t batch_id, runtime_task *owner,
                                      Handler &&handler) {
    return parallel_shards_ordered(shard_begin, sharded_ops, batch_id, ordered_batch_options{},
                                   owner, std::forward<Handler>(handler));
}

template <typename Op, typename Handler>
bool runtime::parallel_shards_ordered(thread_ref shard_begin, ShardedOps<Op> &sharded_ops,
                                      std::uint64_t batch_id, ordered_batch_options options,
                                      runtime_task *owner, Handler &&handler) {
    std::vector<std::uint16_t> threads;
    threads.reserve(sharded_ops.shard_count());
    for (std::uint16_t i = 0; i < sharded_ops.shard_count(); ++i) {
        threads.push_back(static_cast<std::uint16_t>(shard_begin.index + i));
    }
    return parallel_shards_ordered(thread_group_ref(threads.data(), threads.size()), sharded_ops,
                                   batch_id, options, owner, std::forward<Handler>(handler));
}

inline std::uint64_t runtime::ordered_last_applied_batch_id(thread_ref thread) const noexcept {
    if (!valid_thread(thread) || thread.index >= ordered_batch_state_.size()) {
        return 0;
    }
    return ordered_batch_state_[thread.index].last_applied_batch_id;
}

template <typename StreamTag, typename ApplyTaskT, typename Batch>
bool runtime::start_ordered_task(thread_ref sequencer_thread, Batch &&batch) {
    using batch_type = std::decay_t<Batch>;
    try {
        auto task = make_task<ordered_start_task<StreamTag, ApplyTaskT, batch_type>>(
            *this, std::forward<Batch>(batch));
        return task->do_it(sequencer_thread);
    } catch (...) {
        return false;
    }
}

template <typename Op, typename Handler, bool Ordered>
bool runtime::parallel_shards_impl(std::bool_constant<Ordered>, thread_group_ref shard_threads,
                                   ShardedOps<Op> &sharded_ops, parallel_mode mode,
                                   std::uint64_t batch_id, ordered_batch_options options,
                                   runtime_task *owner, Handler &&handler) {
    if (owner == nullptr || current_runtime_ != this || !valid_thread(current_thread_index_)) {
        AF_ASSERT(false && "parallel_shards must run on the owner runtime thread");
        return false;
    }

    const std::uint16_t shard_count = sharded_ops.shard_count();
    if (shard_count > shard_threads.size()) {
        AF_ASSERT(false && "parallel shard thread group is smaller than sharded_ops");
        return false;
    }

    for (std::uint16_t i = 0; i < shard_count; ++i) {
        if (!valid_thread(shard_threads.at(i))) {
            return false;
        }
    }

    std::uint32_t target_count = shard_count;
    if (mode == parallel_mode::non_empty_only) {
        target_count = 0;
        for (std::uint16_t i = 0; i < shard_count; ++i) {
            if (!sharded_ops.shards[i].empty()) {
                ++target_count;
            }
        }
    }

    if (target_count == 0) {
        detail::runtime_task_access::set_last_parallel_failures(owner, 0);
        return detail::runtime_task_access::schedule_to(owner, current_thread_index_);
    }

    detail::RuntimeInstanceParallelGroup *group = nullptr;
    try {
        group = detail::create_runtime_instance_parallel_group(*this, target_count, owner,
                                                               current_thread_index_);
    } catch (...) {
        return false;
    }

    using handler_type = std::decay_t<Handler>;
    for (std::uint16_t i = 0; i < shard_count; ++i) {
        if (mode == parallel_mode::non_empty_only && sharded_ops.shards[i].empty()) {
            continue;
        }

        const thread_ref target = shard_threads.at(i);
        if (target.index == current_thread_index_) {
            auto ops = std::move(sharded_ops.shards[i]);
            handler_type local_handler(handler);
            const bool ok = run_parallel_shard<Op, handler_type, Ordered>(i, batch_id, options, ops,
                                                                          local_handler);
            group->complete(ok);
            continue;
        }

        try {
            auto task = make_task<parallel_shard_task<Op, handler_type, Ordered>>(
                *this, group, i, batch_id, options, std::move(sharded_ops.shards[i]),
                handler_type(handler));
            if (!task->do_it(target)) {
                group->complete(false);
            }
        } catch (...) {
            group->complete(false);
        }
    }
    return true;
}

inline auto runtime::check_order_guard(std::uint64_t batch_id,
                                       ordered_batch_options options) noexcept
    -> ordered_guard_decision {
    const std::uint16_t thread = current_thread_index_;
    if (current_runtime_ != this || thread >= ordered_batch_state_.size()) {
        AF_ASSERT(false && "ordered batch guard must run on an owner runtime thread");
        return ordered_guard_decision::fail;
    }

    auto &state = ordered_batch_state_[thread];
    if (batch_id == state.last_applied_batch_id + 1U) {
        return ordered_guard_decision::run;
    }
    if (options.replay_policy == ordered_batch_replay_policy::skip_already_applied &&
        batch_id == state.last_applied_batch_id) {
        return ordered_guard_decision::skip_already_applied;
    }
    AF_ASSERT(false && "ordered batch id must be contiguous per shard");
    return ordered_guard_decision::fail;
}

inline void runtime::commit_order_guard(std::uint64_t batch_id) noexcept {
    const std::uint16_t thread = current_thread_index_;
    if (current_runtime_ == this && thread < ordered_batch_state_.size()) {
        ordered_batch_state_[thread].last_applied_batch_id = batch_id;
    }
}

template <typename StreamTag, typename ApplyTaskT, typename BatchT>
struct runtime::ordered_start_state {
    runtime *owner{nullptr};
    std::uint64_t next_batch_id{1};
    absl::flat_hash_map<std::uint64_t, BatchT> pending;

    void reset(runtime &runtime_owner) {
        owner = &runtime_owner;
        next_batch_id = 1;
        pending.clear();
    }

    [[nodiscard]] bool submit(runtime &runtime_owner, BatchT batch) {
        if (owner != &runtime_owner) {
            reset(runtime_owner);
        }

        const std::uint64_t batch_id = batch.batch_id;
        if (batch_id < next_batch_id) {
            return true;
        }

        if (batch_id > next_batch_id) {
            pending.emplace(batch_id, std::move(batch));
            return true;
        }

        if (!start_ready(runtime_owner, std::move(batch))) {
            return false;
        }
        return drain_ready(runtime_owner);
    }

    [[nodiscard]] bool start_ready(runtime &runtime_owner, BatchT batch) {
        try {
            auto task = make_task<ApplyTaskT>(runtime_owner);
            if (!task->do_it(std::move(batch))) {
                return false;
            }
            ++next_batch_id;
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool drain_ready(runtime &runtime_owner) {
        for (;;) {
            auto it = pending.find(next_batch_id);
            if (it == pending.end()) {
                return true;
            }

            BatchT batch = std::move(it->second);
            pending.erase(it);
            if (!start_ready(runtime_owner, std::move(batch))) {
                return false;
            }
        }
    }
};

template <typename StreamTag, typename ApplyTaskT, typename BatchT>
auto runtime::ordered_start_state_for_thread()
    -> ordered_start_state<StreamTag, ApplyTaskT, BatchT> & {
    thread_local ordered_start_state<StreamTag, ApplyTaskT, BatchT> state;
    if (state.owner != this) {
        state.reset(*this);
    }
    return state;
}

template <typename StreamTag, typename ApplyTaskT, typename BatchT>
class runtime::ordered_start_task final : public runtime_task {
public:
    ordered_start_task(factory_token token, runtime &owner, BatchT batch)
        : runtime_task(token, owner), batch_(std::move(batch)) {}

    [[nodiscard]] bool do_it(thread_ref sequencer_thread) noexcept {
        return schedule_to(sequencer_thread);
    }

private:
    task_result run_task() noexcept override {
        const bool ok =
            owner().template ordered_start_state_for_thread<StreamTag, ApplyTaskT, BatchT>().submit(
                owner(), std::move(batch_));
        return ok ? done() : failed();
    }

    BatchT batch_{};
};

template <typename Op, typename Handler, bool Ordered>
bool runtime::run_parallel_shard(std::uint16_t shard_index, std::uint64_t batch_id,
                                 ordered_batch_options options, std::vector<Op> &ops,
                                 Handler &handler) noexcept {
    bool ok = true;
    bool skip_handler = false;
    if constexpr (Ordered) {
        const ordered_guard_decision decision = check_order_guard(batch_id, options);
        ok = decision != ordered_guard_decision::fail;
        skip_handler = decision == ordered_guard_decision::skip_already_applied;
    }

    if (ok && !skip_handler) {
        try {
            if constexpr (Ordered) {
                using handler_result = std::invoke_result_t<Handler &, std::uint16_t,
                                                            std::vector<Op> &, std::uint64_t>;
                if constexpr (std::is_same_v<handler_result, bool>) {
                    ok = handler(shard_index, ops, batch_id);
                } else {
                    handler(shard_index, ops, batch_id);
                }
            } else {
                using handler_result =
                    std::invoke_result_t<Handler &, std::uint16_t, std::vector<Op> &>;
                if constexpr (std::is_same_v<handler_result, bool>) {
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

template <typename Op, typename Handler, bool Ordered>
class runtime::parallel_shard_task final : public runtime_task {
public:
    parallel_shard_task(factory_token token, runtime &owner,
                        detail::RuntimeInstanceParallelGroup *group, std::uint16_t shard_index,
                        std::uint64_t batch_id, ordered_batch_options options,
                        std::vector<Op> &&ops, Handler handler)
        : runtime_task(token, owner), group_(group), shard_index_(shard_index), batch_id_(batch_id),
          options_(options), ops_(std::move(ops)), handler_(std::move(handler)) {}

    [[nodiscard]] bool do_it(thread_ref target) noexcept {
        return schedule_to(target);
    }

private:
    task_result run_task() noexcept override {
        const bool ok = owner().template run_parallel_shard<Op, Handler, Ordered>(
            shard_index_, batch_id_, options_, ops_, handler_);
        group_->complete(ok);
        return done();
    }

    detail::RuntimeInstanceParallelGroup *group_{nullptr};
    std::uint16_t shard_index_{0};
    std::uint64_t batch_id_{0};
    ordered_batch_options options_;
    std::vector<Op> ops_;
    Handler handler_;
};

} // namespace af
