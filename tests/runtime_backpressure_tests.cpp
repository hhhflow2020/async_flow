#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_backpressure_runtime_config(std::size_t thread_count) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("backpressure", thread_count)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.diagnostics.enable_thread_name = false;
    return config;
}

template <typename T>
[[nodiscard]] bool wait_until_at_least(const std::atomic<T> &value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

template <typename TaskT, typename... Args>
[[nodiscard]] bool start_task(af::runtime &runtime, Args &&...args) {
    auto task = af::make_task<TaskT>(runtime);
    if (!task->do_it(std::forward<Args>(args)...)) {
        return false;
    }
    return true;
}

class blocking_task final : public af::runtime_task {
public:
    blocking_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &started,
                             std::atomic<bool> &release, std::atomic<int> &completed) noexcept {
        started_ = &started;
        release_ = &release;
        completed_ = &completed;
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        started_->fetch_add(1, std::memory_order_release);
        while (!release_->load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *started_{nullptr};
    std::atomic<bool> *release_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class noop_task final : public af::runtime_task {
public:
    noop_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &completed,
                             std::atomic<int> *destroyed = nullptr) noexcept {
        completed_ = &completed;
        destroyed_ = destroyed;
        return schedule_to(thread);
    }

    ~noop_task() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::task_result run_task() noexcept override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

class same_thread_fanout_task final : public af::runtime_task {
public:
    same_thread_fanout_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, int child_count, std::atomic<int> &completed,
                             std::atomic<int> &accepted) noexcept {
        thread_ = thread;
        child_count_ = child_count;
        completed_ = &completed;
        accepted_ = &accepted;
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        for (int i = 0; i < child_count_; ++i) {
            if (!start_task<noop_task>(owner(), thread_, *completed_)) {
                return failed();
            }
            accepted_->fetch_add(1, std::memory_order_release);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::thread_ref thread_;
    int child_count_{0};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *accepted_{nullptr};
};

} // namespace

TEST(RuntimeBackpressureTests, UnboundedInboxAcceptsTasksBehindBlockingOwner) {
    af::runtime runtime(make_backpressure_runtime_config(1));
    const af::thread_ref thread = runtime.cpu_threads().front();
    ASSERT_TRUE(thread);
    ASSERT_TRUE(runtime.start());

    constexpr int queued_task_count = 16;
    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(start_task<blocking_task>(runtime, thread, started, release, completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    for (int i = 0; i < queued_task_count; ++i) {
        EXPECT_TRUE(start_task<noop_task>(runtime, thread, completed, &destroyed));
    }
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 0);

    release.store(true, std::memory_order_release);
    EXPECT_TRUE(wait_until_at_least(completed, queued_task_count + 1));
    EXPECT_TRUE(wait_until_at_least(destroyed, queued_task_count));

    runtime.stop();
}

TEST(RuntimeBackpressureTests, SameThreadRuntimeProducerUsesUnifiedInbox) {
    af::runtime runtime(make_backpressure_runtime_config(1));
    const af::thread_ref thread = runtime.cpu_threads().front();
    ASSERT_TRUE(thread);
    ASSERT_TRUE(runtime.start());

    constexpr int child_count = 4;
    std::atomic<int> completed{0};
    std::atomic<int> accepted{0};

    ASSERT_TRUE(
        start_task<same_thread_fanout_task>(runtime, thread, child_count, completed, accepted));
    ASSERT_TRUE(wait_until_at_least(completed, child_count + 1));
    EXPECT_EQ(accepted.load(std::memory_order_acquire), child_count);

    runtime.stop();
}

TEST(RuntimeBackpressureTests, UnboundedInboxAllowsManyExternalProducers) {
    af::runtime runtime(make_backpressure_runtime_config(2));
    const af::thread_group_ref threads = runtime.cpu_threads();
    ASSERT_EQ(threads.size(), 2U);
    ASSERT_TRUE(runtime.start());

    constexpr int producer_count = 4;
    constexpr int tasks_per_producer = 200;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([producer, &runtime, threads, &completed, &all_started] {
            for (int i = 0; i < tasks_per_producer; ++i) {
                const af::thread_ref target =
                    threads.at(static_cast<std::size_t>((producer + i) & 1));
                if (!start_task<noop_task>(runtime, target, completed)) {
                    all_started.store(false, std::memory_order_release);
                    return;
                }
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_TRUE(all_started.load(std::memory_order_acquire));
    EXPECT_TRUE(wait_until_at_least(completed, producer_count * tasks_per_producer));

    runtime.stop();
}

TEST(RuntimeBackpressureTests, RuntimeThreadFanoutUsesUnifiedInbox) {
    af::runtime runtime(make_backpressure_runtime_config(2));
    const af::thread_ref thread = runtime.cpu_threads().front();
    ASSERT_TRUE(thread);
    ASSERT_TRUE(runtime.start());

    constexpr int child_count = 128;
    std::atomic<int> completed{0};
    std::atomic<int> accepted{0};

    ASSERT_TRUE(
        start_task<same_thread_fanout_task>(runtime, thread, child_count, completed, accepted));
    EXPECT_TRUE(wait_until_at_least(completed, child_count + 1));
    EXPECT_EQ(accepted.load(std::memory_order_acquire), child_count);

    runtime.stop();
}
