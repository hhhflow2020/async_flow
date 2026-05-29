#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <vector>

#include "app_runtime.hpp"

namespace {

struct AddGoldOp {
    std::uint64_t player_id{0};
    int gold{0};
};

class AddGoldBatchTask final : public Task {
public:
    explicit AddGoldBatchTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        std::vector<AddGoldOp> ops,
        std::atomic<int>* completed,
        std::atomic<int>* total_gold,
        std::array<std::atomic<int>, AppRuntimeTraits::logic_count>* shard_hits) {
        completed_ = completed;
        total_gold_ = total_gold;
        shard_hits_ = shard_hits;
        sharded_ops_ = Runtime::split_by_shard(
            std::move(ops),
            AppRuntimeTraits::logic_count,
            [](const AddGoldOp& op) { return op.player_id; });
        return schedule(AppThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                AppRuntimeTraits::logic_begin,
                sharded_ops_,
                af::ParallelMode::NonEmptyOnly,
                this,
                [this](std::uint16_t shard, std::vector<AddGoldOp>& shard_ops) {
                    (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
                    int local_gold = 0;
                    for (const auto& op : shard_ops) {
                        local_gold += op.gold;
                    }
                    total_gold_->fetch_add(local_gold, std::memory_order_relaxed);
                });
            return pending();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            completed_->notify_one();
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    af::ShardedOps<AddGoldOp> sharded_ops_{AppRuntimeTraits::logic_count};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* total_gold_{nullptr};
    std::array<std::atomic<int>, AppRuntimeTraits::logic_count>* shard_hits_{nullptr};
};

} // namespace

int main() {
    Runtime::init();

    std::atomic<int> completed{0};
    std::atomic<int> total_gold{0};
    std::array<std::atomic<int>, AppRuntimeTraits::logic_count> shard_hits{};

    std::vector<AddGoldOp> ops{
        {1001, 10},
        {1002, 20},
        {1005, 30},
        {1010, 40},
    };

    [[maybe_unused]] const bool started = Runtime::start_task<AddGoldBatchTask>(
        std::move(ops),
        &completed,
        &total_gold,
        &shard_hits);
    AF_ASSERT(started);

    wait_completed(completed, 1);

    std::cout << "parallel total gold: " << total_gold.load(std::memory_order_relaxed) << '\n';
    for (std::uint16_t shard = 0; shard < AppRuntimeTraits::logic_count; ++shard) {
        std::cout << "shard " << shard << " hits: "
                  << shard_hits[shard].load(std::memory_order_relaxed) << '\n';
    }

    Runtime::shutdown();
    return 0;
}
