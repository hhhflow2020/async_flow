#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_parallel_runtime_config() {
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

struct AddOp {
    std::uint64_t key{0};
    int value{0};
};

class InstanceParallelOwnerTask final : public af::runtime_task {
public:
    InstanceParallelOwnerTask(factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads, std::atomic<int> &completed,
                             std::atomic<int> &total, std::atomic<std::uint32_t> &failures) {
        logic_threads_ = logic_threads;
        completed_ = &completed;
        total_ = &total;
        failures_ = &failures;
        std::vector<AddOp> ops{{1, 10}, {2, 20}, {5, 30}};
        sharded_ops_ = af::runtime::split_by_shard(
            std::move(ops), static_cast<std::uint16_t>(logic_threads_.size()),
            [](const AddOp &op) { return op.key; });
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        launch,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::launch:
            state_ = state::finish;
            return owner().parallel_shards(logic_threads_, sharded_ops_,
                                           af::parallel_mode::non_empty_only, this,
                                           [this](std::uint16_t, std::vector<AddOp> &ops) {
                                               int local = 0;
                                               for (const AddOp &op : ops) {
                                                   local += op.value;
                                               }
                                               total_->fetch_add(local, std::memory_order_relaxed);
                                           })
                       ? pending()
                       : failed();

        case state::finish:
            failures_->store(last_parallel_failures(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    state state_{state::launch};
    af::thread_group_ref logic_threads_;
    af::sharded_ops<AddOp> sharded_ops_;
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *total_{nullptr};
    std::atomic<std::uint32_t> *failures_{nullptr};
};

struct OrderedStream {};

struct OrderedBatch {
    std::uint64_t batch_id{0};
    std::vector<int> values;
    std::atomic<int> *completed{nullptr};
    std::atomic<int> *total{nullptr};
};

class InstanceOrderedApplyTask final : public af::runtime_task {
public:
    InstanceOrderedApplyTask(factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(OrderedBatch batch) {
        batch_ = std::move(batch);
        logic_threads_ = owner().thread_group("logic");
        if (logic_threads_.empty()) {
            return false;
        }
        sharded_values_ = af::sharded_ops<int>(static_cast<std::uint16_t>(logic_threads_.size()));
        for (std::size_t i = 0; i < batch_.values.size(); ++i) {
            sharded_values_.shards[i % logic_threads_.size()].push_back(batch_.values[i]);
        }
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        apply,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::apply:
            state_ = state::finish;
            return owner().parallel_shards_ordered(
                       logic_threads_, sharded_values_, batch_.batch_id, this,
                       [this](std::uint16_t, std::vector<int> &values, std::uint64_t) {
                           int local = 0;
                           for (int value : values) {
                               local += value;
                           }
                           batch_.total->fetch_add(local, std::memory_order_relaxed);
                       })
                       ? pending()
                       : failed();

        case state::finish:
            if (last_parallel_failures() != 0) {
                return failed();
            }
            batch_.completed->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    state state_{state::apply};
    OrderedBatch batch_;
    af::thread_group_ref logic_threads_;
    af::sharded_ops<int> sharded_values_;
};

class InstanceOrderedSubmitTask final : public af::runtime_task {
public:
    InstanceOrderedSubmitTask(factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(OrderedBatch batch, af::thread_ref io_thread) {
        batch_ = std::move(batch);
        return schedule_to(io_thread);
    }

private:
    af::task_result run_task() noexcept override {
        const af::thread_group_ref logic_threads = owner().thread_group("logic");
        const bool started = owner().start_ordered_task<OrderedStream, InstanceOrderedApplyTask>(
            logic_threads.front(), std::move(batch_));
        return started ? done() : failed();
    }

    OrderedBatch batch_;
};

} // namespace

TEST(RuntimeInstanceParallelTests, ParallelGroupKeepsPendingOwnerAliveAfterHandleRelease) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());

    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::atomic<int> total{0};
    std::atomic<std::uint32_t> failures{0};
    {
        auto task = af::make_task<InstanceParallelOwnerTask>(runtime);
        ASSERT_TRUE(task->do_it(logic_threads, completed, total, failures));
    }

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    EXPECT_EQ(total.load(std::memory_order_relaxed), 60);
    EXPECT_EQ(failures.load(std::memory_order_acquire), 0U);
}

TEST(RuntimeInstanceParallelTests, OrderedStartBuffersOutOfOrderBatches) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());

    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    const af::thread_group_ref io_threads = runtime.thread_group("io");
    ASSERT_EQ(logic_threads.size(), 4U);
    ASSERT_FALSE(io_threads.empty());

    std::atomic<int> completed{0};
    std::atomic<int> total{0};
    auto second = af::make_task<InstanceOrderedSubmitTask>(runtime);
    auto first = af::make_task<InstanceOrderedSubmitTask>(runtime);
    ASSERT_TRUE(second->do_it(OrderedBatch{2, {5, 6}, &completed, &total}, io_threads.front()));
    ASSERT_TRUE(
        first->do_it(OrderedBatch{1, {1, 2, 3, 4}, &completed, &total}, io_threads.front()));

    EXPECT_TRUE(wait_for_counter(completed, 2));

    EXPECT_EQ(total.load(std::memory_order_relaxed), 21);
    for (std::size_t i = 0; i < logic_threads.size(); ++i) {
        EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(i)), 2U);
    }
    runtime.stop();
}
