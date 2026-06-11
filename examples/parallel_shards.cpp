#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "app_runtime.hpp"

namespace {

struct add_gold_op {
    std::uint64_t player_id{0};
    int gold{0};
};

[[nodiscard]] bool wait_for_counter(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) >= expected;
}

class add_gold_batch_task final : public af::runtime_task {
public:
    add_gold_batch_task(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(std::vector<add_gold_op> ops, std::atomic<int> &total_gold,
                             std::array<std::atomic<int>, player_logic_shard_count> &shard_hits,
                             std::atomic<int> &completed, af::thread_group_ref logic_threads) {
        total_gold_ = &total_gold;
        shard_hits_ = &shard_hits;
        completed_ = &completed;
        logic_threads_ = logic_threads;
        sharded_ops_ = af::runtime::split_by_shard(
            std::move(ops), static_cast<std::uint16_t>(logic_threads_.size()),
            [](const add_gold_op &op) { return op.player_id; });
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split:
            return split_to_shards();

        case state::finish:
            return finish();
        }

        return failed();
    }

    af::task_result split_to_shards() noexcept {
        state_ = state::finish;
        const bool started = owner().parallel_shards(
            logic_threads_, sharded_ops_, af::parallel_mode::non_empty_only, this,
            [this](std::uint16_t shard, std::vector<add_gold_op> &shard_ops) {
                apply_shard(shard, shard_ops);
            });
        return started ? pending() : failed();
    }

    void apply_shard(std::uint16_t shard, const std::vector<add_gold_op> &shard_ops) noexcept {
        (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
        int local_gold = 0;
        for (const auto &op : shard_ops) {
            local_gold += op.gold;
        }
        total_gold_->fetch_add(local_gold, std::memory_order_relaxed);
    }

    af::task_result finish() noexcept {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    state state_{state::split};
    af::thread_group_ref logic_threads_;
    af::sharded_ops<add_gold_op> sharded_ops_;
    std::atomic<int> *total_gold_{nullptr};
    std::array<std::atomic<int>, player_logic_shard_count> *shard_hits_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

} // namespace

int main() {
    af::runtime runtime(make_app_runtime_config());
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        return 1;
    }

    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    if (logic_threads.size() != player_logic_shard_count) {
        std::cerr << "runtime thread layout is missing logic shards\n";
        runtime.stop();
        return 1;
    }

    std::atomic<int> total_gold{0};
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, player_logic_shard_count> shard_hits{};

    std::vector<add_gold_op> ops{
        {1001, 10},
        {1002, 20},
        {1005, 30},
        {1010, 40},
    };

    auto task = af::make_task<add_gold_batch_task>(runtime);
    const bool started =
        task->do_it(std::move(ops), total_gold, shard_hits, completed, logic_threads);
    const bool completed_all = wait_for_counter(completed, 1);
    runtime.stop();

    std::cout << "parallel total gold: " << total_gold.load(std::memory_order_relaxed) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        std::cout << "shard " << shard
                  << " hits: " << shard_hits[shard].load(std::memory_order_relaxed) << '\n';
    }

    return started && completed_all && total_gold.load(std::memory_order_relaxed) == 100 ? 0 : 1;
}
