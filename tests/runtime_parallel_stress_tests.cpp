#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "support/runtime_scheduler_stress_support.hpp"

namespace {

using namespace af::test::runtime_scheduler_stress;

} // namespace

TEST(RuntimeStressTests, ParallelShardOwnerResumesUnderBursts) {
    ParallelResumeRuntime::shutdown();
    ParallelResumeRuntime::init();

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
            if (!ParallelResumeRuntime::start_task<ParallelResumeTask>(
                    i, &remaining, &completed, &failures, &shard_runs, &sum, task_stage.data(),
                    task_shards.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "ParallelResumeRuntime::start_task failed at burst " << burst
                              << " task " << i;
                ParallelResumeRuntime::shutdown();
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
            ParallelResumeRuntime::shutdown();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(shard_runs.load(std::memory_order_acquire), tasks_per_burst * 4)
            << "burst=" << burst;
        ASSERT_EQ(sum.load(std::memory_order_acquire), expected_task_sum * tasks_per_burst)
            << "burst=" << burst;
    }

    ParallelResumeRuntime::shutdown();
}
