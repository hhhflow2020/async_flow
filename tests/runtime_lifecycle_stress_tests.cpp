#include <array>
#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "support/runtime_lifecycle_stress_support.hpp"

namespace {

using namespace af::test::runtime_lifecycle_stress;

} // namespace

TEST(RuntimeStressTests, ConcurrentInitShutdownAndStartTask) {
    StressRuntime::shutdown();

    StressCounters counters;
    std::atomic<bool> stop{false};
    constexpr int producer_count = 4;
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([producer, &stop, &counters] {
            int seed = producer;
            while (!stop.load(std::memory_order_acquire)) {
                if (StressRuntime::start_task<StressTask>(seed, &counters)) {
                    counters.accepted.fetch_add(1, std::memory_order_release);
                } else {
                    counters.rejected.fetch_add(1, std::memory_order_release);
                }
                seed += producer_count;
                if ((seed & 63) == 0) {
                    std::this_thread::yield();
                }
            }
        });
    }

    int cycles = 0;
    const auto deadline = std::chrono::steady_clock::now() + stress_duration();
    while (std::chrono::steady_clock::now() < deadline) {
        StressRuntime::init();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        StressRuntime::shutdown();
        ++cycles;
    }

    stop.store(true, std::memory_order_release);
    for (auto& producer : producers) {
        producer.join();
    }
    StressRuntime::shutdown();

    EXPECT_GT(cycles, 0);
    EXPECT_GT(counters.configured.load(std::memory_order_acquire), 0);
    EXPECT_GT(counters.accepted.load(std::memory_order_acquire), 0);
    EXPECT_GT(counters.pending_entered.load(std::memory_order_acquire), 0);
    EXPECT_EQ(
        counters.destroyed.load(std::memory_order_acquire),
        counters.configured.load(std::memory_order_acquire));
}
