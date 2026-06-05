#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <thread>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

[[nodiscard]] std::chrono::milliseconds stress_duration() {
    if (const char *value = std::getenv("ASYNCFLOW_STRESS_MS")) {
        const int parsed = std::atoi(value);
        if (parsed > 0) {
            return std::chrono::milliseconds(parsed);
        }
    }
    return std::chrono::milliseconds(300);
}

[[nodiscard]] af::runtime_config make_lifecycle_stress_runtime_config() {
    af::runtime_config config;
    config.threads = {af::cpu_threads("lifecycle-stress", 4)};
    config.logger.consumer_thread = af::thread_selector::any_cpu();
    config.diagnostics.enable_thread_name = false;
    return config;
}

struct stress_counters {
    std::atomic<int> configured{0};
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};
    std::atomic<int> completed{0};
    std::atomic<int> pending_entered{0};
    std::atomic<int> destroyed{0};
};

class lifecycle_stress_task final : public af::runtime_task {
public:
    lifecycle_stress_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    ~lifecycle_stress_task() override {
        if (counters_ != nullptr) {
            counters_->destroyed.fetch_add(1, std::memory_order_release);
        }
    }

    [[nodiscard]] bool do_it(int seed, af::thread_group_ref threads,
                             stress_counters &counters) noexcept {
        threads_ = threads;
        counters_ = &counters;
        mode_ = seed % 3;
        state_ = state::start;
        counters_->configured.fetch_add(1, std::memory_order_release);
        return schedule_to(threads_.at(static_cast<std::uint16_t>(seed & 3)));
    }

private:
    enum class state : std::uint8_t {
        start,
        hop,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::start:
            if (mode_ == 0) {
                counters_->completed.fetch_add(1, std::memory_order_release);
                return done();
            }
            if (mode_ == 1) {
                state_ = state::hop;
                const auto current_offset = static_cast<std::uint16_t>(
                    af::runtime::current_thread_index() - threads_.front().index);
                const af::thread_ref next = threads_.at(
                    static_cast<std::uint16_t>((current_offset + 1U) % threads_.size()));
                return pending_to(next);
            }
            counters_->pending_entered.fetch_add(1, std::memory_order_release);
            return pending();

        case state::hop:
            counters_->completed.fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    af::thread_group_ref threads_;
    state state_{state::start};
    int mode_{0};
    stress_counters *counters_{nullptr};
};

} // namespace

TEST(RuntimeStressTests, ConcurrentInitShutdownAndStartTask) {
    af::runtime runtime(make_lifecycle_stress_runtime_config());
    const af::thread_group_ref threads = runtime.thread_group("lifecycle-stress");
    ASSERT_EQ(threads.size(), 4U);

    stress_counters counters;
    std::atomic<bool> stop{false};
    constexpr int producer_count = 4;
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([producer, &runtime, threads, &stop, &counters] {
            int seed = producer;
            while (!stop.load(std::memory_order_acquire)) {
                if (!runtime.running()) {
                    counters.rejected.fetch_add(1, std::memory_order_release);
                    std::this_thread::yield();
                    continue;
                }
                auto task = af::make_task<lifecycle_stress_task>(runtime);
                if (task->do_it(seed, threads, counters)) {
                    counters.accepted.fetch_add(1, std::memory_order_release);
                    task.reset();
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
        static_cast<void>(runtime.start());
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        runtime.stop();
        ++cycles;
    }

    stop.store(true, std::memory_order_release);
    for (auto &producer : producers) {
        producer.join();
    }
    runtime.stop();

    EXPECT_GT(cycles, 0);
    EXPECT_GT(counters.configured.load(std::memory_order_acquire), 0);
    EXPECT_GT(counters.accepted.load(std::memory_order_acquire), 0);
    EXPECT_GT(counters.pending_entered.load(std::memory_order_acquire), 0);
    EXPECT_EQ(counters.destroyed.load(std::memory_order_acquire),
              counters.configured.load(std::memory_order_acquire));
}
