#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_ordered_start_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", 4),
        af::io_threads("io", 1),
    };
    return config;
}

[[nodiscard]] bool wait_for_counter(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) >= expected;
}

struct OrderedStartStream {};
struct OrderedStartFailureStream {};

struct OrderedStartBatch {
    std::uint64_t batch_id{0};
    int value{0};
    std::vector<int> *applied{nullptr};
    std::atomic<int> *completed{nullptr};
};

class InstanceOrderedStartApplyTask final : public af::runtime_task {
public:
    InstanceOrderedStartApplyTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(OrderedStartBatch batch) {
        batch_ = batch;
        const af::thread_group_ref logic_threads = owner().thread_group("logic");
        if (logic_threads.empty()) {
            return false;
        }
        return schedule_to(logic_threads.front());
    }

private:
    af::task_result run_task() noexcept override {
        batch_.applied->push_back(batch_.value);
        batch_.completed->fetch_add(1, std::memory_order_release);
        return done();
    }

    OrderedStartBatch batch_;
};

struct OrderedStartFailureBatch {
    std::uint64_t batch_id{0};
    int value{0};
    std::vector<int> *applied{nullptr};
    std::atomic<int> *attempts{nullptr};
    std::atomic<int> *completed{nullptr};
    std::atomic<bool> *fail_first_start{nullptr};
};

class InstanceOrderedStartFailingApplyTask final : public af::runtime_task {
public:
    InstanceOrderedStartFailingApplyTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(OrderedStartFailureBatch batch) {
        batch.attempts->fetch_add(1, std::memory_order_release);
        if (batch.batch_id == 1U &&
            !batch.fail_first_start->exchange(true, std::memory_order_acq_rel)) {
            return false;
        }

        batch_ = batch;
        const af::thread_group_ref logic_threads = owner().thread_group("logic");
        if (logic_threads.empty()) {
            return false;
        }
        return schedule_to(logic_threads.front());
    }

private:
    af::task_result run_task() noexcept override {
        batch_.applied->push_back(batch_.value);
        batch_.completed->fetch_add(1, std::memory_order_release);
        return done();
    }

    OrderedStartFailureBatch batch_;
};

} // namespace

TEST(RuntimeInstanceOrderedStartTests, BuffersOutOfOrderBatches) {
    af::runtime runtime(make_ordered_start_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{2, 20, &applied, &completed})));
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{3, 30, &applied, &completed})));

    ASSERT_TRUE(wait_for_counter(completed, 3));
    runtime.stop();

    ASSERT_EQ(applied.size(), 3U);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
    EXPECT_EQ(applied[2], 30);
}

TEST(RuntimeInstanceOrderedStartTests, KeepsGapBufferedUntilMissingBatchArrives) {
    af::runtime runtime(make_ordered_start_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{2, 20, &applied, &completed})));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);
    EXPECT_TRUE(applied.empty());

    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE(wait_for_counter(completed, 2));
    runtime.stop();

    ASSERT_EQ(applied.size(), 2U);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST(RuntimeInstanceOrderedStartTests, IgnoresDuplicateAndOldBatches) {
    af::runtime runtime(make_ordered_start_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> completed{0};
    std::vector<int> applied;

    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{1, 10, &applied, &completed})));
    ASSERT_TRUE(wait_for_counter(completed, 1));
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{1, 100, &applied, &completed})));
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{2, 20, &applied, &completed})));
    ASSERT_TRUE(wait_for_counter(completed, 2));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    runtime.stop();

    EXPECT_EQ(completed.load(std::memory_order_acquire), 2);
    ASSERT_EQ(applied.size(), 2U);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST(RuntimeInstanceOrderedStartTests, DoesNotAdvanceWhenApplyStartFails) {
    af::runtime runtime(make_ordered_start_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> attempts{0};
    std::atomic<int> completed{0};
    std::atomic<bool> fail_first_start{false};
    std::vector<int> applied;

    ASSERT_TRUE((
        runtime.start_ordered_task<OrderedStartFailureStream, InstanceOrderedStartFailingApplyTask>(
            io_threads.front(),
            OrderedStartFailureBatch{1, 10, &applied, &attempts, &completed, &fail_first_start})));
    ASSERT_TRUE(wait_for_counter(attempts, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);

    ASSERT_TRUE((
        runtime.start_ordered_task<OrderedStartFailureStream, InstanceOrderedStartFailingApplyTask>(
            io_threads.front(),
            OrderedStartFailureBatch{2, 20, &applied, &attempts, &completed, &fail_first_start})));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(attempts.load(std::memory_order_acquire), 1);
    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);

    ASSERT_TRUE((
        runtime.start_ordered_task<OrderedStartFailureStream, InstanceOrderedStartFailingApplyTask>(
            io_threads.front(),
            OrderedStartFailureBatch{1, 10, &applied, &attempts, &completed, &fail_first_start})));
    ASSERT_TRUE(wait_for_counter(completed, 2));
    runtime.stop();

    ASSERT_EQ(applied.size(), 2U);
    EXPECT_EQ(applied[0], 10);
    EXPECT_EQ(applied[1], 20);
}

TEST(RuntimeInstanceOrderedStartTests, StateResetsAfterRuntimeRestart) {
    af::runtime runtime(make_ordered_start_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> first_completed{0};
    std::vector<int> first_applied;
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        io_threads.front(), OrderedStartBatch{1, 10, &first_applied, &first_completed})));
    ASSERT_TRUE(wait_for_counter(first_completed, 1));
    runtime.stop();

    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref restarted_io_threads = runtime.thread_group("io");
    ASSERT_FALSE(restarted_io_threads.empty());
    std::atomic<int> second_completed{0};
    std::vector<int> second_applied;
    ASSERT_TRUE((runtime.start_ordered_task<OrderedStartStream, InstanceOrderedStartApplyTask>(
        restarted_io_threads.front(),
        OrderedStartBatch{1, 20, &second_applied, &second_completed})));
    ASSERT_TRUE(wait_for_counter(second_completed, 1));
    runtime.stop();

    ASSERT_EQ(first_applied.size(), 1U);
    ASSERT_EQ(second_applied.size(), 1U);
    EXPECT_EQ(first_applied[0], 10);
    EXPECT_EQ(second_applied[0], 20);
}
