#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_parallel_ordered_start_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    struct OrderedStartState {
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
            const bool ok = AsyncRuntime::start_task<ApplyTaskT>(std::move(batch));
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

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    [[nodiscard]] static OrderedStartState<StreamTag, ApplyTaskT, BatchT>& ordered_start_state() {
        static std::vector<OrderedStartState<StreamTag, ApplyTaskT, BatchT>> states(thread_count);
        const std::uint16_t index = current_thread_index();
        AF_ASSERT(index < states.size());
        auto& state = states[index];
        const std::uint64_t runtime_generation = generation_.load(std::memory_order_acquire);
        if (state.generation != runtime_generation) {
            state.reset(runtime_generation);
        }
        return state;
    }

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    class OrderedStartTask final : public Task {
    public:
        explicit OrderedStartTask(typename Task::FactoryToken token) : Task(token) {}

        bool do_it(Thread sequencer_thread, BatchT batch) {
            batch_ = std::move(batch);
            return this->schedule(sequencer_thread);
        }

    private:
        TaskResult run() override {
            const bool ok = ordered_start_state<StreamTag, ApplyTaskT, BatchT>().submit(
                std::move(batch_));
            return ok ? this->done() : this->failed();
        }

        BatchT batch_{};
    };
