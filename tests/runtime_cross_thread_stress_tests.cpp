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

TEST(RuntimeStressTests, RepeatedCrossThreadHopBurstsComplete) {
    RepeatHopRuntime::shutdown();
    RepeatHopRuntime::init();

    constexpr int burst_count = 64;
    constexpr int tasks_per_burst = 1024;
    constexpr int hops_per_task = 8;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> runs{0};
        std::atomic<int> post_failures{0};
        std::array<std::atomic<int>, tasks_per_burst> progress{};
        std::array<std::atomic<int>, tasks_per_burst> last_thread{};
        for (int i = 0; i < tasks_per_burst; ++i) {
            last_thread[static_cast<std::size_t>(i)].store(-1, std::memory_order_relaxed);
            remaining.fetch_add(1, std::memory_order_relaxed);
            if (!RepeatHopRuntime::start_task<RepeatHopTask>(hops_per_task, i, &remaining, &runs,
                                                             &post_failures, progress.data(),
                                                             last_thread.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RepeatHopRuntime::start_task failed at burst " << burst;
                RepeatHopRuntime::shutdown();
                return;
            }
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        if (!wait_zero_until(remaining, deadline)) {
            int min_progress = hops_per_task + 1;
            int min_id = -1;
            int min_thread = -1;
            for (int i = 0; i < tasks_per_burst; ++i) {
                const int value =
                    progress[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
                if (value < min_progress) {
                    min_progress = value;
                    min_id = i;
                    min_thread =
                        last_thread[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
                }
            }
            ADD_FAILURE() << "cross-thread hop burst did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " runs=" << runs.load(std::memory_order_acquire)
                          << " post_failures=" << post_failures.load(std::memory_order_acquire)
                          << " min_id=" << min_id << " min_progress=" << min_progress
                          << " last_thread=" << min_thread
                          << " expected_runs=" << tasks_per_burst * (hops_per_task + 1);
            RepeatHopRuntime::shutdown();
            return;
        }
        ASSERT_EQ(post_failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
    }

    RepeatHopRuntime::shutdown();
}

TEST(RuntimeStressTests, AboveSixtyFourThreadCrossWordHopCompletes) {
    WideHopRuntime::shutdown();
    WideHopRuntime::init();

    constexpr int task_count = 128;
    constexpr int hops_per_task = 6;
    std::atomic<int> remaining{0};
    std::atomic<int> runs{0};
    std::atomic<int> post_failures{0};

    for (int i = 0; i < task_count; ++i) {
        remaining.fetch_add(1, std::memory_order_relaxed);
        if (!WideHopRuntime::start_task<WideHopTask>(hops_per_task, &remaining, &runs,
                                                     &post_failures)) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "WideHopRuntime::start_task failed at task " << i;
            WideHopRuntime::shutdown();
            return;
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    if (!wait_zero_until(remaining, deadline)) {
        ADD_FAILURE() << "wide hop tasks did not drain, remaining="
                      << remaining.load(std::memory_order_acquire)
                      << " runs=" << runs.load(std::memory_order_acquire)
                      << " post_failures=" << post_failures.load(std::memory_order_acquire)
                      << " expected_runs=" << task_count * (hops_per_task + 1);
        WideHopRuntime::shutdown();
        return;
    }

    EXPECT_EQ(post_failures.load(std::memory_order_acquire), 0);
    EXPECT_EQ(runs.load(std::memory_order_acquire), task_count * (hops_per_task + 1));
    WideHopRuntime::shutdown();
}
