#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>

#include <gtest/gtest.h>

#include "support/runtime_scheduler_stress_support.hpp"

namespace {

using namespace af::test::runtime_scheduler_stress;

void run_terminal_wake_case(RunningWakeTerminalMode mode) {
    RunningPendingRuntime::shutdown();
    RunningPendingRuntime::init();

    constexpr int burst_count = 32;
    constexpr int tasks_per_burst = 32;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> completed{0};
        std::atomic<int> wake_attempts{0};
        std::atomic<int> failures{0};
        std::array<std::atomic<int>, tasks_per_burst> wake_flags{};

        for (int i = 0; i < tasks_per_burst; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            if (!RunningPendingRuntime::start_task<RunningWakeTerminalOwnerTask>(
                    mode, i, &remaining, &completed, &wake_attempts, &failures,
                    wake_flags.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RunningPendingRuntime::start_task failed at burst " << burst
                              << " task " << i;
                RunningPendingRuntime::shutdown();
                return;
            }
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            ADD_FAILURE() << "terminal running wake did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " completed=" << completed.load(std::memory_order_acquire)
                          << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire);
            RunningPendingRuntime::shutdown();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0)
            << "burst=" << burst << " completed=" << completed.load(std::memory_order_acquire)
            << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire);
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(wake_attempts.load(std::memory_order_acquire), tasks_per_burst)
            << "burst=" << burst;
    }

    RunningPendingRuntime::shutdown();
}

} // namespace

TEST(RuntimeStressTests, RunningToPendingWakeDoesNotStrandOwner) {
    RunningPendingRuntime::shutdown();
    RunningPendingRuntime::init();

    constexpr int burst_count = 128;
    constexpr int tasks_per_burst = 64;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> completed{0};
        std::atomic<int> wake_attempts{0};
        std::atomic<int> failures{0};
        std::array<std::atomic<int>, tasks_per_burst> stages{};
        std::array<std::atomic<int>, tasks_per_burst> wake_flags{};

        for (int i = 0; i < tasks_per_burst; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            if (!RunningPendingRuntime::start_task<RunningPendingOwnerTask>(
                    i, &remaining, &completed, &wake_attempts, &failures, stages.data(),
                    wake_flags.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RunningPendingRuntime::start_task failed at burst " << burst
                              << " task " << i;
                RunningPendingRuntime::shutdown();
                return;
            }
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            int min_stage = 4;
            int min_id = -1;
            for (int i = 0; i < tasks_per_burst; ++i) {
                const int stage =
                    stages[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
                if (stage < min_stage) {
                    min_stage = stage;
                    min_id = i;
                }
            }
            ADD_FAILURE() << "running-to-pending wake did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " completed=" << completed.load(std::memory_order_acquire)
                          << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire)
                          << " min_id=" << min_id << " min_stage=" << min_stage;
            RunningPendingRuntime::shutdown();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(wake_attempts.load(std::memory_order_acquire), tasks_per_burst)
            << "burst=" << burst;
    }

    RunningPendingRuntime::shutdown();
}

TEST(RuntimeStressTests, RunningWakeBeforeDoneIsBenign) {
    run_terminal_wake_case(RunningWakeTerminalMode::Done);
}

TEST(RuntimeStressTests, RunningWakeBeforeAgainIsBenign) {
    run_terminal_wake_case(RunningWakeTerminalMode::Again);
}
