#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "app_runtime.hpp"

namespace {

struct player_delta_stream {};

struct player_delta_batch {
    std::uint64_t batch_id{0};
    std::vector<int> deltas;
    std::atomic<int> *total_delta{nullptr};
    std::array<std::atomic<std::uint64_t>, player_logic_shard_count> *shard_batch_seen{nullptr};
    std::atomic<int> *completed_batches{nullptr};
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

class apply_player_delta_batch_task final : public af::runtime_task {
public:
    apply_player_delta_batch_task(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(player_delta_batch batch) {
        batch_ = std::move(batch);
        logic_threads_ = owner().thread_group("logic");
        if (logic_threads_.empty()) {
            return false;
        }
        sharded_deltas_ = af::sharded_ops<int>(static_cast<std::uint16_t>(logic_threads_.size()));
        for (std::size_t i = 0; i < batch_.deltas.size(); ++i) {
            sharded_deltas_.shards[i % logic_threads_.size()].push_back(batch_.deltas[i]);
        }
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
        const bool started = owner().parallel_shards_ordered(
            logic_threads_, sharded_deltas_, batch_.batch_id, this,
            [this](std::uint16_t shard, std::vector<int> &deltas, std::uint64_t batch_id) {
                apply_shard(shard, deltas, batch_id);
            });
        return started ? pending() : failed();
    }

    void apply_shard(std::uint16_t shard, const std::vector<int> &deltas,
                     std::uint64_t batch_id) noexcept {
        (*batch_.shard_batch_seen)[shard].store(batch_id, std::memory_order_release);
        int local_delta = 0;
        for (int delta : deltas) {
            local_delta += delta;
        }
        batch_.total_delta->fetch_add(local_delta, std::memory_order_relaxed);
    }

    af::task_result finish() noexcept {
        batch_.completed_batches->fetch_add(1, std::memory_order_release);
        return done();
    }

    state state_{state::apply};
    player_delta_batch batch_;
    af::thread_group_ref logic_threads_;
    af::sharded_ops<int> sharded_deltas_;
};

class submit_player_delta_batch_task final : public af::runtime_task {
public:
    submit_player_delta_batch_task(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(player_delta_batch batch, af::thread_ref io_thread) {
        batch_ = std::move(batch);
        return schedule_to(io_thread);
    }

private:
    af::task_result run_task() noexcept override {
        const af::thread_group_ref logic_threads = owner().thread_group("logic");
        if (logic_threads.empty()) {
            return failed();
        }
        const bool started =
            owner().start_ordered_task<player_delta_stream, apply_player_delta_batch_task>(
                logic_threads.front(), std::move(batch_));
        return started ? done() : failed();
    }

    player_delta_batch batch_;
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

    std::atomic<int> total_delta{0};
    std::atomic<int> completed_batches{0};
    std::array<std::atomic<std::uint64_t>, player_logic_shard_count> shard_batch_seen{};

    auto second_task = af::make_task<submit_player_delta_batch_task>(runtime);
    auto first_task = af::make_task<submit_player_delta_batch_task>(runtime);

    const bool second_started = second_task->do_it(
        player_delta_batch{2, {5, 6}, &total_delta, &shard_batch_seen, &completed_batches},
        io_threads.front());
    const bool first_started = first_task->do_it(
        player_delta_batch{1, {1, 2, 3, 4}, &total_delta, &shard_batch_seen, &completed_batches},
        io_threads.front());

    const bool completed_all = wait_for_counter(completed_batches, 2);
    runtime.stop();

    std::cout << "ordered total delta: " << total_delta.load(std::memory_order_relaxed) << '\n';
    for (std::uint16_t shard = 0; shard < player_logic_shard_count; ++shard) {
        std::cout << "shard " << shard
                  << " last batch: " << shard_batch_seen[shard].load(std::memory_order_acquire)
                  << '\n';
    }

    return first_started && second_started && completed_all &&
                   total_delta.load(std::memory_order_relaxed) == 21
               ? 0
               : 1;
}
