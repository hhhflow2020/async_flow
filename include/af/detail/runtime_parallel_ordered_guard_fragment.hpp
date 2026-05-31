#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_parallel_ordered_guard_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    enum class OrderedGuardDecision : std::uint8_t {
        Run,
        SkipAlreadyApplied,
        Fail,
    };

    [[nodiscard]] static OrderedGuardDecision check_order_guard(
        std::uint64_t batch_id,
        OrderedBatchOptions options) noexcept {
        const std::uint16_t thread = AsyncRuntime::current_thread_index();
        AF_ASSERT(thread < ordered_batch_state_.size());
        auto& state = ordered_batch_state_[thread];
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

    static void commit_order_guard(std::uint64_t batch_id) noexcept {
        const std::uint16_t thread = AsyncRuntime::current_thread_index();
        ordered_batch_state_[thread].last_applied_batch_id = batch_id;
    }
