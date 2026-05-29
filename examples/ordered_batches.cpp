#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <vector>

#include "app_runtime.hpp"

namespace {

struct PlayerDeltaStream {};

struct PlayerDeltaBatch {
    std::uint64_t batch_id{0};
    std::vector<int> deltas;
    std::atomic<int>* completed{nullptr};
    std::atomic<int>* total_delta{nullptr};
    std::array<std::atomic<std::uint64_t>, player_logic_shard_count>* shard_batch_seen{nullptr};
};

class ApplyPlayerDeltaBatchTask final : public Task {
public:
    explicit ApplyPlayerDeltaBatchTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(PlayerDeltaBatch batch) {
        batch_ = std::move(batch);
        sharded_deltas_ = af::ShardedOps<int>(player_logic_shard_count);
        for (std::size_t i = 0; i < batch_.deltas.size(); ++i) {
            sharded_deltas_.shards[i % player_logic_shard_count].push_back(batch_.deltas[i]);
        }
        return schedule(AppThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            state_ = State::Finish;
            Flow::parallel_shards_ordered(
                player_logic_begin,
                sharded_deltas_,
                batch_.batch_id,
                this,
                [this](std::uint16_t shard, std::vector<int>& deltas, std::uint64_t batch_id) {
                    (*batch_.shard_batch_seen)[shard].store(batch_id, std::memory_order_release);
                    int local_delta = 0;
                    for (int delta : deltas) {
                        local_delta += delta;
                    }
                    batch_.total_delta->fetch_add(local_delta, std::memory_order_relaxed);
                });
            return pending();

        case State::Finish:
            batch_.completed->fetch_add(1, std::memory_order_release);
            batch_.completed->notify_one();
            return done();
        }

        return failed();
    }

    State state_{State::Apply};
    PlayerDeltaBatch batch_;
    af::ShardedOps<int> sharded_deltas_{player_logic_shard_count};
};

} // namespace

int main() {
    Flow::init();

    std::atomic<int> completed{0};
    std::atomic<int> total_delta{0};
    std::array<std::atomic<std::uint64_t>, player_logic_shard_count> shard_batch_seen{};

    [[maybe_unused]] const bool second_started =
        Flow::start_ordered_task<PlayerDeltaStream, ApplyPlayerDeltaBatchTask>(
            AppThread::Logic_0,
            PlayerDeltaBatch{2, {5, 6}, &completed, &total_delta, &shard_batch_seen});
    [[maybe_unused]] const bool first_started =
        Flow::start_ordered_task<PlayerDeltaStream, ApplyPlayerDeltaBatchTask>(
            AppThread::Logic_0,
            PlayerDeltaBatch{1, {1, 2, 3, 4}, &completed, &total_delta, &shard_batch_seen});
    AF_ASSERT(second_started);
    AF_ASSERT(first_started);

    wait_completed(completed, 2);

    std::cout << "ordered total delta: " << total_delta.load(std::memory_order_relaxed) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        std::cout << "shard " << shard << " last batch: "
                  << shard_batch_seen[shard].load(std::memory_order_acquire) << '\n';
    }

    Flow::shutdown();
    return 0;
}
