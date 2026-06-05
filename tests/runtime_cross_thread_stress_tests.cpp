#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_hop_runtime_config(const char *name,
                                                         std::size_t thread_count) {
    af::runtime_config config;
    config.threads = {af::cpu_threads(name, thread_count)};
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

class RepeatHopTask final : public af::runtime_task {
public:
    RepeatHopTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int hops, int id, af::thread_ref first, af::thread_ref second,
                             std::atomic<int> &remaining, std::atomic<int> &runs,
                             std::atomic<int> &post_failures, std::atomic<int> *progress,
                             std::atomic<int> *last_thread) noexcept {
        hops_ = hops;
        id_ = id;
        first_ = first;
        second_ = second;
        remaining_ = &remaining;
        runs_ = &runs;
        post_failures_ = &post_failures;
        progress_ = progress;
        last_thread_ = last_thread;
        return schedule_to(first_);
    }

private:
    af::task_result run_task() noexcept override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        progress_[id_].fetch_add(1, std::memory_order_relaxed);

        const std::uint16_t current = af::runtime::current_thread_index();
        last_thread_[id_].store(current == first_.index ? 0 : 1, std::memory_order_relaxed);
        if (hops_-- > 0) {
            const af::thread_ref next = current == first_.index ? second_ : first_;
            if (!schedule_to(next)) {
                record_schedule_failure();
                return failed();
            }
            return pending();
        }

        complete();
        return done();
    }

    void record_schedule_failure() noexcept {
        post_failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    int hops_{0};
    int id_{0};
    af::thread_ref first_{};
    af::thread_ref second_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *runs_{nullptr};
    std::atomic<int> *post_failures_{nullptr};
    std::atomic<int> *progress_{nullptr};
    std::atomic<int> *last_thread_{nullptr};
};

class WideHopTask final : public af::runtime_task {
public:
    WideHopTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int hops, af::thread_ref first, af::thread_ref second,
                             std::atomic<int> &remaining, std::atomic<int> &runs,
                             std::atomic<int> &post_failures) noexcept {
        hops_ = hops;
        first_ = first;
        second_ = second;
        remaining_ = &remaining;
        runs_ = &runs;
        post_failures_ = &post_failures;
        return schedule_to(first_);
    }

private:
    af::task_result run_task() noexcept override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        if (hops_-- > 0) {
            const af::thread_ref next =
                af::runtime::current_thread_index() == first_.index ? second_ : first_;
            if (!schedule_to(next)) {
                record_schedule_failure();
                return failed();
            }
            return pending();
        }

        complete();
        return done();
    }

    void record_schedule_failure() noexcept {
        post_failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    int hops_{0};
    af::thread_ref first_{};
    af::thread_ref second_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *runs_{nullptr};
    std::atomic<int> *post_failures_{nullptr};
};

} // namespace

TEST(RuntimeStressTests, RepeatedCrossThreadHopBurstsComplete) {
    af::runtime runtime(make_hop_runtime_config("repeat-hop", 2));
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref threads = runtime.cpu_threads();
    ASSERT_EQ(threads.size(), 2U);

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
            auto task = af::make_task<RepeatHopTask>(runtime);
            if (!task->do_it(hops_per_task, i, threads.at(0), threads.at(1), remaining, runs,
                             post_failures, progress.data(), last_thread.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RepeatHopTask::do_it failed at burst " << burst;
                runtime.stop();
                return;
            }
            task.reset();
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
            runtime.stop();
            return;
        }
        ASSERT_EQ(post_failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
    }

    runtime.stop();
}

TEST(RuntimeStressTests, AboveSixtyFourThreadCrossWordHopCompletes) {
    af::runtime runtime(make_hop_runtime_config("wide-hop", 65));
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref threads = runtime.cpu_threads();
    ASSERT_EQ(threads.size(), 65U);

    constexpr int task_count = 128;
    constexpr int hops_per_task = 6;
    std::atomic<int> remaining{0};
    std::atomic<int> runs{0};
    std::atomic<int> post_failures{0};

    for (int i = 0; i < task_count; ++i) {
        remaining.fetch_add(1, std::memory_order_relaxed);
        auto task = af::make_task<WideHopTask>(runtime);
        if (!task->do_it(hops_per_task, threads.at(0), threads.at(64), remaining, runs,
                         post_failures)) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "WideHopTask::do_it failed at task " << i;
            runtime.stop();
            return;
        }
        task.reset();
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    if (!wait_zero_until(remaining, deadline)) {
        ADD_FAILURE() << "wide hop tasks did not drain, remaining="
                      << remaining.load(std::memory_order_acquire)
                      << " runs=" << runs.load(std::memory_order_acquire)
                      << " post_failures=" << post_failures.load(std::memory_order_acquire)
                      << " expected_runs=" << task_count * (hops_per_task + 1);
        runtime.stop();
        return;
    }

    EXPECT_EQ(post_failures.load(std::memory_order_acquire), 0);
    EXPECT_EQ(runs.load(std::memory_order_acquire), task_count * (hops_per_task + 1));
    runtime.stop();
}
