#include <array>
#include <atomic>
#include <chrono>

#include <gtest/gtest.h>

#include "support/runtime_scheduler_stress_support.hpp"

namespace {

using namespace af::test::runtime_scheduler_stress;

} // namespace

TEST(RuntimeStressTests, SameThreadFanoutUsesUnifiedInboxAndPreservesFifo) {
    SelfPostRuntime::shutdown();
    SelfPostRuntime::init();

    constexpr int burst_count = 64;
    constexpr int child_count = 256;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{1};
        std::atomic<int> failures{0};
        std::atomic<int> sequence{0};
        std::atomic<int> root_completed{0};
        std::array<std::atomic<int>, child_count> order{};
        for (auto &value : order) {
            value.store(-1, std::memory_order_relaxed);
        }

        if (!SelfPostRuntime::start_task<SelfPostFanoutTask>(
                child_count, &remaining, &failures, &sequence, order.data(), &root_completed)) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "SelfPostRuntime::start_task failed at burst " << burst;
            SelfPostRuntime::shutdown();
            return;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            ADD_FAILURE() << "same-thread fanout did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire)
                          << " sequence=" << sequence.load(std::memory_order_acquire)
                          << " root_completed=" << root_completed.load(std::memory_order_acquire);
            SelfPostRuntime::shutdown();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(sequence.load(std::memory_order_acquire), child_count) << "burst=" << burst;
        for (int id = 0; id < child_count; ++id) {
            ASSERT_EQ(order[static_cast<std::size_t>(id)].load(std::memory_order_acquire), id)
                << "burst=" << burst << " position=" << id;
        }
    }

    SelfPostRuntime::shutdown();
}

TEST(RuntimeStressTests, SameThreadAgainUsesUnifiedInboxWithoutCrossThreadHints) {
    SelfPostRuntime::shutdown();
    SelfPostRuntime::init();

    constexpr int task_count = 128;
    constexpr int iteration_count = 64;
    std::atomic<int> remaining{task_count};
    std::array<std::atomic<int>, task_count> runs{};

    for (int i = 0; i < task_count; ++i) {
        if (!SelfPostRuntime::start_task<SelfAgainTask>(iteration_count, &remaining,
                                                        &runs[static_cast<std::size_t>(i)])) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "SelfPostRuntime::start_task failed at task " << i;
            SelfPostRuntime::shutdown();
            return;
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (!wait_zero_until(remaining, deadline)) {
        int min_runs = iteration_count;
        int min_id = -1;
        for (int i = 0; i < task_count; ++i) {
            const int value = runs[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
            if (value < min_runs) {
                min_runs = value;
                min_id = i;
            }
        }
        ADD_FAILURE() << "same-thread again tasks did not drain, remaining="
                      << remaining.load(std::memory_order_acquire) << " min_id=" << min_id
                      << " min_runs=" << min_runs;
        SelfPostRuntime::shutdown();
        return;
    }

    for (int i = 0; i < task_count; ++i) {
        EXPECT_EQ(runs[static_cast<std::size_t>(i)].load(std::memory_order_acquire),
                  iteration_count)
            << "task=" << i;
    }
    SelfPostRuntime::shutdown();
}
