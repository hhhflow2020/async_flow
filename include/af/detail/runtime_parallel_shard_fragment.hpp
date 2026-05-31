#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_parallel_shard_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] static bool run_parallel_shard(
        std::uint16_t shard_index,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        std::vector<Op>& ops,
        Handler& handler) noexcept {
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
                    using HandlerResult =
                        std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&, std::uint64_t>;
                    if constexpr (std::is_same_v<HandlerResult, bool>) {
                        ok = handler(shard_index, ops, batch_id);
                    } else {
                        handler(shard_index, ops, batch_id);
                    }
                } else {
                    using HandlerResult =
                        std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&>;
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

    template <typename Op, typename Handler, bool Ordered>
    class ShardTask final : public Task {
    public:
        ShardTask(
            typename Task::FactoryToken token,
            ParallelGroup* group,
            std::uint16_t shard_index,
            std::uint64_t batch_id,
            OrderedBatchOptions options,
            std::vector<Op>&& ops,
            Handler handler)
            : Task(token),
              group_(group),
              shard_index_(shard_index),
              batch_id_(batch_id),
              options_(options),
              ops_(std::move(ops)),
              handler_(std::move(handler)) {}

    private:
        TaskResult run() override {
            const bool ok = run_parallel_shard<Op, Handler, Ordered>(
                shard_index_,
                batch_id_,
                options_,
                ops_,
                handler_);
            group_->complete(ok);
            return this->done();
        }

        void on_runtime_cancel() noexcept override {
            group_->complete(false, false);
        }

        ParallelGroup* group_;
        std::uint16_t shard_index_;
        std::uint64_t batch_id_;
        OrderedBatchOptions options_;
        std::vector<Op> ops_;
        Handler handler_;
    };
