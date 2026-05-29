#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <thread>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

namespace {

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
