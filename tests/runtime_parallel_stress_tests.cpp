#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_parallel_resume_runtime_config() {
    af::runtime_config config;
    config.threads = {af::cpu_threads("parallel-resume", 4)};
    config.logger.consumer_thread = af::thread_selector::any_cpu();
    config.diagnostics.enable_thread_name = false;
    return config;
}

[[nodiscard]] bool wait_zero_until(std::atomic<int> &value,
                                   std::chrono::steady_clock::time_point deadline) {
    while (value.load(std::memory_order_acquire) != 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return value.load(std::memory_order_acquire) == 0;
        }

        const int observed = value.load(std::memory_order_acquire);
        if (observed == 0) {
            return true;
        }
        const auto remaining = deadline - now;
        const auto wait_for =
            remaining < std::chrono::milliseconds(1) ? remaining : std::chrono::milliseconds(1);
        static_cast<void>(af::detail::atomic_wait_value_for(value, observed, wait_for,
                                                            std::memory_order_acquire));
    }
    return true;
}

class ParallelResumeTask final : public af::runtime_task {
public:
    ParallelResumeTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int id, af::thread_group_ref logic_threads,
                             std::atomic<int> &remaining, std::atomic<int> &completed,
                             std::atomic<int> &failures, std::atomic<int> &shard_runs,
                             std::atomic<std::uint64_t> &sum, std::atomic<int> *task_stage,
                             std::atomic<int> *task_shards) {
        id_ = id;
        logic_threads_ = logic_threads;
        remaining_ = &remaining;
        completed_ = &completed;
        failures_ = &failures;
        shard_runs_ = &shard_runs;
        sum_ = &sum;
        task_stage_ = task_stage;
        task_shards_ = task_shards;
        task_stage_[id_].store(1, std::memory_order_relaxed);
        ops_ = af::sharded_ops<std::uint64_t>(4);
        for (std::uint64_t value = 0; value < 1024; ++value) {
            ops_.shards[value & 3U].push_back(value);
        }
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
            return launch_shards();
        case state::finish:
            return finish_owner();
        }

        failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
        return failed();
    }

    af::task_result launch_shards() noexcept {
        state_ = state::finish;
        task_stage_[id_].store(2, std::memory_order_relaxed);
        const bool launched = owner().parallel_shards(
            logic_threads_, ops_, af::parallel_mode::all_shards, this,
            [this](std::uint16_t shard, std::vector<std::uint64_t> &shard_ops) {
                task_shards_[id_ * 4 + shard].fetch_add(1, std::memory_order_relaxed);
                shard_runs_->fetch_add(1, std::memory_order_relaxed);
                std::uint64_t local = 0;
                for (const std::uint64_t value : shard_ops) {
                    local += value;
                }
                sum_->fetch_add(local, std::memory_order_relaxed);
            });
        task_stage_[id_].store(3, std::memory_order_relaxed);
        if (!launched) {
            failures_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return failed();
        }
        return pending();
    }

    af::task_result finish_owner() noexcept {
        task_stage_[id_].store(4, std::memory_order_relaxed);
        if (last_parallel_failures() != 0U) {
            failures_->fetch_add(1, std::memory_order_relaxed);
        }
        completed_->fetch_add(1, std::memory_order_relaxed);
        complete();
        return done();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    state state_{state::split};
    af::thread_group_ref logic_threads_;
    af::sharded_ops<std::uint64_t> ops_{4};
    int id_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *shard_runs_{nullptr};
    std::atomic<std::uint64_t> *sum_{nullptr};
    std::atomic<int> *task_stage_{nullptr};
    std::atomic<int> *task_shards_{nullptr};
};

} // namespace

TEST(RuntimeStressTests, ParallelShardOwnerResumesUnderBursts) {
    af::runtime runtime(make_parallel_resume_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("parallel-resume");
    ASSERT_EQ(logic_threads.size(), 4U);

    constexpr int burst_count = 64;
    constexpr int tasks_per_burst = 128;
    constexpr std::uint64_t expected_task_sum = 1023ULL * 1024ULL / 2ULL;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> completed{0};
        std::atomic<int> failures{0};
        std::atomic<int> shard_runs{0};
        std::atomic<std::uint64_t> sum{0};
        std::array<std::atomic<int>, tasks_per_burst> task_stage{};
        std::array<std::atomic<int>, tasks_per_burst * 4> task_shards{};

        for (int i = 0; i < tasks_per_burst; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<ParallelResumeTask>(runtime);
            if (!task->do_it(i, logic_threads, remaining, completed, failures, shard_runs, sum,
                             task_stage.data(), task_shards.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "ParallelResumeTask::do_it failed at burst " << burst << " task "
                              << i;
                runtime.stop();
                return;
            }
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            int min_stage = 4;
            int min_id = -1;
            int min_shards = 4;
            for (int i = 0; i < tasks_per_burst; ++i) {
                const int stage =
                    task_stage[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
                int task_shard_count = 0;
                for (int shard = 0; shard < 4; ++shard) {
                    task_shard_count += task_shards[static_cast<std::size_t>(i * 4 + shard)].load(
                        std::memory_order_acquire);
                }
                if (stage < min_stage || (stage == min_stage && task_shard_count < min_shards)) {
                    min_stage = stage;
                    min_id = i;
                    min_shards = task_shard_count;
                }
            }
            ADD_FAILURE() << "parallel shard owner resumes did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " completed=" << completed.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire)
                          << " shard_runs=" << shard_runs.load(std::memory_order_acquire)
                          << " sum=" << sum.load(std::memory_order_acquire) << " min_id=" << min_id
                          << " min_stage=" << min_stage << " min_shards=" << min_shards;
            runtime.stop();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(shard_runs.load(std::memory_order_acquire), tasks_per_burst * 4)
            << "burst=" << burst;
        ASSERT_EQ(sum.load(std::memory_order_acquire), expected_task_sum * tasks_per_burst)
            << "burst=" << burst;
    }

    runtime.stop();
}
