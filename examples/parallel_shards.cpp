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

    bool do_it(std::vector<AddGoldOp> ops, std::atomic<int> *total_gold,
               std::array<std::atomic<int>, player_logic_shard_count> *shard_hits) {
        total_gold_ = total_gold;
        shard_hits_ = shard_hits;
        sharded_ops_ = async::split_by_shard(std::move(ops), player_logic_shard_count,
                                             [](const AddGoldOp &op) { return op.player_id; });
        return schedule_to(AppThreads::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::task_result run() override {
        switch (state_) {
        case State::Split:
            return split_to_shards();

        case State::Finish:
            return finish();
        }

        return failed();
    }

    af::task_result split_to_shards() {
        state_ = State::Finish;
        async::parallel_shards(player_logic_begin, sharded_ops_, af::parallel_mode::non_empty_only,
                               this,
                               [this](std::uint16_t shard, std::vector<AddGoldOp> &shard_ops) {
                                   apply_shard(shard, shard_ops);
                               });
        return pending();
    }

    void apply_shard(std::uint16_t shard, const std::vector<AddGoldOp> &shard_ops) {
        (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
        int local_gold = 0;
        for (const auto &op : shard_ops) {
            local_gold += op.gold;
        }
        total_gold_->fetch_add(local_gold, std::memory_order_relaxed);
    }

    af::task_result finish() {
        return done();
    }

    State state_{State::Split};
    af::sharded_ops<AddGoldOp> sharded_ops_{player_logic_shard_count};
    std::atomic<int> *total_gold_{nullptr};
    std::array<std::atomic<int>, player_logic_shard_count> *shard_hits_{nullptr};
};

} // namespace

int main() {
    async::init();

    std::atomic<int> total_gold{0};
    std::array<std::atomic<int>, player_logic_shard_count> shard_hits{};

    std::vector<AddGoldOp> ops{
        {1001, 10},
        {1002, 20},
        {1005, 30},
        {1010, 40},
    };

    {
        auto task = async::make_task<AddGoldBatchTask>();
        [[maybe_unused]] const bool started = task->do_it(std::move(ops), &total_gold, &shard_hits);
        AF_ASSERT(started);
    }

    async::shutdown();

    std::cout << "parallel total gold: " << total_gold.load(std::memory_order_relaxed) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        std::cout << "shard " << shard
                  << " hits: " << shard_hits[shard].load(std::memory_order_relaxed) << '\n';
    }

    return 0;
}
