#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <thread>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"
#include "support/runtime_scheduler_stress_support.hpp"

namespace {

using namespace af::test::runtime_scheduler_stress;

std::chrono::milliseconds stress_duration() {
    if (const char* value = std::getenv("ASYNCFLOW_STRESS_MS")) {
        const int parsed = std::atoi(value);
        if (parsed > 0) {
            return std::chrono::milliseconds(parsed);
        }
    }
    return std::chrono::milliseconds(300);
}

enum class StressThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    enum_thread_index_end,
};

struct StressRuntimeTraits {
    using Thread = StressThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(StressThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 8192;
    static constexpr std::size_t external_queue_capacity = 8192;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using StressRuntime = af::AsyncRuntime<StressRuntimeTraits>;
using StressTaskBase = StressRuntime::Task;

struct StressCounters {
    std::atomic<int> configured{0};
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};
    std::atomic<int> completed{0};
    std::atomic<int> pending_entered{0};
    std::atomic<int> destroyed{0};
};

class StressTask final : public StressTaskBase {
public:
    explicit StressTask(StressTaskBase::FactoryToken token) : StressTaskBase(token) {}

    bool do_it(int seed, StressCounters* counters) {
        counters_ = counters;
        mode_ = seed % 3;
        state_ = State::Start;
        counters_->configured.fetch_add(1, std::memory_order_release);
        return schedule(static_cast<StressThread>(seed & 3));
    }

    ~StressTask() override {
        if (counters_ != nullptr) {
            counters_->destroyed.fetch_add(1, std::memory_order_release);
        }
    }

private:
    enum class State : std::uint8_t {
        Start,
        Hop,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            if (mode_ == 0) {
                counters_->completed.fetch_add(1, std::memory_order_release);
                return done();
            }
            if (mode_ == 1) {
                state_ = State::Hop;
                const auto next = static_cast<StressThread>(
                    (StressRuntime::current_thread_index() + 1U) %
                    StressRuntime::thread_count);
                return pending_on(next);
            }
            counters_->pending_entered.fetch_add(1, std::memory_order_release);
            return pending();

        case State::Hop:
            counters_->completed.fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Start};
    int mode_{0};
    StressCounters* counters_{nullptr};
};

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
            if (!RepeatHopRuntime::start_task<RepeatHopTask>(
                    hops_per_task,
                    i,
                    &remaining,
                    &runs,
                    &post_failures,
                    progress.data(),
                    last_thread.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    remaining.notify_one();
                }
                ADD_FAILURE() << "RepeatHopRuntime::start_task failed at burst " << burst;
                RepeatHopRuntime::shutdown();
                return;
            }
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            int min_progress = hops_per_task + 1;
            int min_id = -1;
            int min_thread = -1;
            for (int i = 0; i < tasks_per_burst; ++i) {
                const int value = progress[static_cast<std::size_t>(i)].load(
                    std::memory_order_acquire);
                if (value < min_progress) {
                    min_progress = value;
                    min_id = i;
                    min_thread = last_thread[static_cast<std::size_t>(i)].load(
                        std::memory_order_acquire);
                }
            }
            ADD_FAILURE() << "cross-thread hop burst did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " runs=" << runs.load(std::memory_order_acquire)
                          << " post_failures="
                          << post_failures.load(std::memory_order_acquire)
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
        if (!WideHopRuntime::start_task<WideHopTask>(
                hops_per_task,
                &remaining,
                &runs,
                &post_failures)) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                remaining.notify_one();
            }
            ADD_FAILURE() << "WideHopRuntime::start_task failed at task " << i;
            WideHopRuntime::shutdown();
            return;
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
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
                    i,
                    &remaining,
                    &completed,
                    &failures,
                    &shard_runs,
                    &sum,
                    task_stage.data(),
                    task_shards.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    remaining.notify_one();
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
                const int stage = task_stage[static_cast<std::size_t>(i)].load(
                    std::memory_order_acquire);
                int task_shard_count = 0;
                for (int shard = 0; shard < 4; ++shard) {
                    task_shard_count +=
                        task_shards[static_cast<std::size_t>(i * 4 + shard)].load(
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
                          << " sum=" << sum.load(std::memory_order_acquire)
                          << " min_id=" << min_id << " min_stage=" << min_stage
                          << " min_shards=" << min_shards;
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
