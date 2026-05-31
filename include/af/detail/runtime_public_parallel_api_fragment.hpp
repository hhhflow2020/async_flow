#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_parallel_api_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <Thread Begin, std::uint16_t Count, typename Key>
    [[nodiscard]] static constexpr Thread shard_by(Key key) noexcept {
        static_assert(Count > 0);
        const auto begin = thread_index(Begin);
        const auto value = static_cast<std::uint64_t>(key);
        const auto shard = static_cast<std::uint16_t>(value % Count);
        return thread_from_index(static_cast<std::uint16_t>(begin + shard));
    }

    template <typename Op, typename KeyFn>
    [[nodiscard]] static ShardedOps<Op> split_by_shard(
        std::vector<Op>&& ops,
        std::uint16_t shard_count,
        KeyFn&& key_fn) {
        AF_ASSERT(shard_count > 0);
        ShardedOps<Op> sharded(shard_count);
        if (shard_count == 0) {
            return sharded;
        }

        std::vector<std::size_t> shard_sizes(shard_count, 0);
        std::vector<std::uint16_t> shard_indexes;
        shard_indexes.reserve(ops.size());
        for (auto& op : ops) {
            const auto key = static_cast<std::uint64_t>(key_fn(op));
            const std::uint16_t shard = static_cast<std::uint16_t>(key % shard_count);
            shard_indexes.push_back(shard);
            ++shard_sizes[shard];
        }

        for (std::uint16_t shard = 0; shard < shard_count; ++shard) {
            sharded.shards[shard].reserve(shard_sizes[shard]);
        }

        std::size_t index = 0;
        for (auto& op : ops) {
            sharded.shards[shard_indexes[index++]].push_back(std::move(op));
        }
        return sharded;
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<false>{},
            shard_begin,
            sharded_ops,
            mode,
            0,
            OrderedBatchOptions{},
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        Task* owner,
        Handler&& handler) {
        parallel_shards(
            thread_from_index(0),
            sharded_ops,
            mode,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<true>{},
            shard_begin,
            sharded_ops,
            ParallelMode::AllShards,
            batch_id,
            OrderedBatchOptions{},
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<true>{},
            shard_begin,
            sharded_ops,
            ParallelMode::AllShards,
            batch_id,
            options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        AF_ASSERT(mode == ParallelMode::AllShards);
        static_cast<void>(mode);
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename StreamTag, typename ApplyTaskT, typename Batch>
    [[nodiscard]] static bool start_ordered_task(Thread sequencer_thread, Batch&& batch) {
        using BatchT = std::decay_t<Batch>;
        auto* task = allocate_task<OrderedStartTask<StreamTag, ApplyTaskT, BatchT>>();
        const bool ok = task->do_it(sequencer_thread, std::forward<Batch>(batch));
        if (!ok && task->is_created()) {
            task->release_lifetime_ref();
        }
        return ok;
    }

    [[nodiscard]] static std::uint64_t ordered_last_applied_batch_id(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= ordered_batch_state_.size()) {
            AF_ASSERT(false && "invalid ordered batch thread");
            return 0;
        }
        return ordered_batch_state_[index].last_applied_batch_id;
    }

