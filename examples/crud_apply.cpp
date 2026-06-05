#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"

#include "af/crud_batch.hpp"
#include "app_runtime.hpp"

namespace {

struct PlayerProfile {
    std::uint32_t level{1};
    int gold{0};
};

using PlayerChangeBatch = af::ChangeBatch<std::uint64_t, PlayerProfile>;
using PlayerStore = absl::flat_hash_map<std::uint64_t, PlayerProfile>;

struct PlayerCrudStream {};

std::array<PlayerStore, player_logic_shard_count> player_stores;
std::atomic<int> applied_batches{0};

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

class ApplyPlayerCrudBatchTask final : public af::runtime_task {
public:
    ApplyPlayerCrudBatchTask(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(PlayerChangeBatch batch) {
        batch_ = std::move(batch);
        logic_threads_ = owner().thread_group("logic");
        if (logic_threads_.empty()) {
            return false;
        }
        state_ = state::apply;
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        apply,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::apply:
            return apply_batch();

        case state::finish:
            return finish();
        }

        return failed();
    }

    af::task_result apply_batch() noexcept {
        state_ = state::finish;
        sharded_ops_ =
            af::split_change_batch(batch_, static_cast<std::uint16_t>(logic_threads_.size()));
        const bool started = owner().parallel_shards_ordered(
            logic_threads_, sharded_ops_, batch_.batch_id, af::retryable_ordered_batch_options,
            this,
            [](std::uint16_t shard, auto &ops, std::uint64_t) { return apply_shard(shard, ops); });
        return started ? pending() : failed();
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

    af::task_result finish() noexcept {
        if (last_parallel_failures() == 0) {
            applied_batches.fetch_add(1, std::memory_order_release);
            return done();
        }
        std::cout << "batch " << batch_.batch_id << " failed on " << last_parallel_failures()
                  << " shard(s)\n";
        return failed();
    }

    state state_{state::apply};
    af::thread_group_ref logic_threads_;
    PlayerChangeBatch batch_;
    af::sharded_ops<af::CrudOp<std::uint64_t, PlayerProfile>> sharded_ops_;
};

class SubmitPlayerCrudBatchTask final : public af::runtime_task {
public:
    SubmitPlayerCrudBatchTask(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(PlayerChangeBatch batch, af::thread_ref io_thread) {
        batch_ = std::move(batch);
        return schedule_to(io_thread);
    }

private:
    af::task_result run_task() noexcept override {
        const af::thread_group_ref io_threads = owner().thread_group("io");
        if (io_threads.empty()) {
            return failed();
        }
        const bool ok = owner().start_ordered_task<PlayerCrudStream, ApplyPlayerCrudBatchTask>(
            io_threads.front(), std::move(batch_));
        return ok ? done() : failed();
    }

    PlayerChangeBatch batch_;
};

} // namespace

int main() {
    af::runtime runtime(make_app_runtime_config());
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        return 1;
    }

    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    if (logic_threads.size() != player_logic_shard_count || io_threads.empty()) {
        std::cerr << "runtime thread layout is missing logic or io threads\n";
        runtime.stop();
        return 1;
    }

    auto second_task = af::make_task<SubmitPlayerCrudBatchTask>(runtime);
    auto first_task = af::make_task<SubmitPlayerCrudBatchTask>(runtime);

    const bool second_started = second_task->do_it(
        PlayerChangeBatch{
            2,
            {
                {af::OpType::Update, 1001U, {3, 250}},
                {af::OpType::Add, 1003U, {1, 30}},
            },
        },
        io_threads.front());
    const bool first_started = first_task->do_it(
        PlayerChangeBatch{
            1,
            {
                {af::OpType::Add, 1001U, {2, 100}},
                {af::OpType::Add, 1002U, {5, 500}},
            },
        },
        io_threads.front());

    const bool completed_all = wait_for_counter(applied_batches, 2);
    runtime.stop();

    std::cout << "applied batches: " << applied_batches.load(std::memory_order_acquire) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        for (const auto &[player_id, profile] : player_stores[shard]) {
            std::cout << "shard " << shard << " player " << player_id << " level " << profile.level
                      << " gold " << profile.gold << '\n';
        }
    }

    return first_started && second_started && completed_all ? 0 : 1;
}
