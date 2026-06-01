#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_ordered_start_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

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
