#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>

#include "af/async_flow.hpp"

namespace af::test::runtime_lifecycle_stress {

inline std::chrono::milliseconds stress_duration() {
    if (const char *value = std::getenv("ASYNCFLOW_STRESS_MS")) {
        const int parsed = std::atoi(value);
        if (parsed > 0) {
            return std::chrono::milliseconds(parsed);
        }
    }
    return std::chrono::milliseconds(300);
}

struct StressThreadTag;

struct StressRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<StressThreadTag, 4>());
    static constexpr std::size_t spsc_queue_capacity = 8192;
    static constexpr std::size_t external_queue_capacity = 8192;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using StressRuntime = af::AsyncRuntime<StressRuntimeTraits>;
using StressTaskBase = StressRuntime::Task;
using StressThread = StressRuntime::Thread;

inline constexpr auto stress_threads = StressRuntime::thread_group<StressThreadTag>();

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

    bool do_it(int seed, StressCounters *counters) {
        counters_ = counters;
        mode_ = seed % 3;
        state_ = State::Start;
        counters_->configured.fetch_add(1, std::memory_order_release);
        return schedule(stress_threads.at(static_cast<std::uint16_t>(seed & 3)));
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
                const auto next = StressRuntime::thread_from_index(static_cast<std::uint16_t>(
                    (StressRuntime::current_thread_index() + 1U) % StressRuntime::thread_count));
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
    StressCounters *counters_{nullptr};
};

} // namespace af::test::runtime_lifecycle_stress
