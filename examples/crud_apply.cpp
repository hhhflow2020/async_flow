#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "app_runtime.hpp"

struct PlayerProfile {
    std::uint32_t level{1};
    int gold{0};
};

using PlayerChangeBatch = af::ChangeBatch<std::uint64_t, PlayerProfile>;
using PlayerStore = std::unordered_map<std::uint64_t, PlayerProfile>;

struct PlayerCrudStream {};

std::array<PlayerStore, player_logic_shard_count> player_stores;
std::atomic<int> applied_batches{0};

class ApplyPlayerCrudBatchTask final : public Task {
public:
    explicit ApplyPlayerCrudBatchTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(PlayerChangeBatch batch) {
        batch_ = std::move(batch);
        state_ = State::Apply;
        return schedule(player_logic_begin);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            return apply_batch();

        case State::Finish:
            return finish();
        }

        return failed();
    }

    af::TaskResult apply_batch() {
        state_ = State::Finish;
        sharded_ops_ = af::split_change_batch(batch_, player_logic_shard_count);
        async::parallel_shards_ordered(
            player_logic_begin, sharded_ops_, batch_.batch_id, af::retryable_ordered_batch_options,
            this,
            [](std::uint16_t shard, auto &ops, std::uint64_t) { return apply_shard(shard, ops); });
        return pending();
    }

    static bool apply_shard(std::uint16_t shard,
                            const std::vector<af::CrudOp<std::uint64_t, PlayerProfile>> &ops) {
        auto &store = player_stores[shard];
        for (const auto &op : ops) {
            if (!apply_op(store, op)) {
                return false;
            }
        }
        return true;
    }

    static bool apply_op(PlayerStore &store, const af::CrudOp<std::uint64_t, PlayerProfile> &op) {
        switch (op.type) {
        case af::OpType::Add:
            store[op.key] = op.value;
            return true;

        case af::OpType::Update: {
            auto it = store.find(op.key);
            if (it == store.end()) {
                return false;
            }
            it->second = op.value;
            return true;
        }

        case af::OpType::Delete:
            store.erase(op.key);
            return true;
        }

        return false;
    }

    af::TaskResult finish() {
        if (last_parallel_failures() == 0) {
            applied_batches.fetch_add(1, std::memory_order_release);
            return done();
        }
        std::cout << "batch " << batch_.batch_id << " failed on " << last_parallel_failures()
                  << " shard(s)\n";
        return failed();
    }

    State state_{State::Apply};
    PlayerChangeBatch batch_;
    af::ShardedOps<af::CrudOp<std::uint64_t, PlayerProfile>> sharded_ops_{player_logic_shard_count};
};

class SubmitPlayerCrudBatchTask final : public Task {
public:
    explicit SubmitPlayerCrudBatchTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(PlayerChangeBatch batch) {
        batch_ = std::move(batch);
        return schedule(AppThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const bool ok = async::start_ordered_task<PlayerCrudStream, ApplyPlayerCrudBatchTask>(
            AppThreads::IO_0, std::move(batch_));
        return ok ? done() : failed();
    }

    PlayerChangeBatch batch_;
};

int main() {
    async::init();

    [[maybe_unused]] const bool second_started =
        async::start_task<SubmitPlayerCrudBatchTask>(PlayerChangeBatch{
            2,
            {
                {af::OpType::Update, 1001U, {3, 250}},
                {af::OpType::Add, 1003U, {1, 30}},
            },
        });
    [[maybe_unused]] const bool first_started =
        async::start_task<SubmitPlayerCrudBatchTask>(PlayerChangeBatch{
            1,
            {
                {af::OpType::Add, 1001U, {2, 100}},
                {af::OpType::Add, 1002U, {5, 500}},
            },
        });
    AF_ASSERT(first_started);
    AF_ASSERT(second_started);

    async::shutdown();

    std::cout << "applied batches: " << applied_batches.load(std::memory_order_acquire) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        for (const auto &[player_id, profile] : player_stores[shard]) {
            std::cout << "shard " << shard << " player " << player_id << " level " << profile.level
                      << " gold " << profile.gold << '\n';
        }
    }

    return 0;
}
