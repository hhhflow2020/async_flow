#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_parallel_dispatch_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename Op, typename Handler>
    static void parallel_shards_impl(
        std::bool_constant<false>,
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, false>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
            ordered_options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_impl(
        std::bool_constant<true>,
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, true>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
            ordered_options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler, bool Ordered>
    static void parallel_shards_impl_typed(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
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

        auto* group = create_parallel_group(target_count, owner, current_thread_index());

        using HandlerT = std::decay_t<Handler>;
        for (std::uint16_t i = 0; i < shard_count; ++i) {
            if (mode == ParallelMode::NonEmptyOnly && sharded_ops.shards[i].empty()) {
                continue;
            }

            const Thread thread = thread_from_index(static_cast<std::uint16_t>(begin + i));
            if (thread_index(thread) == current_thread_index()) {
                auto ops = std::move(sharded_ops.shards[i]);
                HandlerT local_handler(handler);
                const bool ok = run_parallel_shard<Op, HandlerT, Ordered>(
                    i,
                    batch_id,
                    ordered_options,
                    ops,
                    local_handler);
                group->complete(ok);
                continue;
            }

            Task* shard_task = nullptr;
            if constexpr (Ordered) {
                shard_task = allocate_task<ShardTask<Op, HandlerT, true>>(
                    group,
                    i,
                    batch_id,
                    ordered_options,
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            } else {
                shard_task = allocate_task<ShardTask<Op, HandlerT, false>>(
                    group,
                    i,
                    0,
                    OrderedBatchOptions{},
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            }

            post_blocking(thread, shard_task);
        }
    }
