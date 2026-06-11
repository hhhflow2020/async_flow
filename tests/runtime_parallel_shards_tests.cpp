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

class InstanceParallelShardTask final : public af::runtime_task {
public:
    InstanceParallelShardTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads, af::parallel_mode mode,
                             std::atomic<int> &completed,
                             std::array<std::atomic<int>, 4> &shard_hits, std::atomic<int> &sum) {
        logic_threads_ = logic_threads;
        mode_ = mode;
        completed_ = &completed;
        shard_hits_ = &shard_hits;
        sum_ = &sum;

        ops_ = af::sharded_ops<int>(4);
        ops_.shards[0] = {1};
        ops_.shards[2] = {2, 3};
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split:
            state_ = state::finish;
            return owner().parallel_shards(
                       logic_threads_, ops_, mode_, this,
                       [this](std::uint16_t shard, std::vector<int> &shard_ops) {
                           (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
                           int local_sum = 0;
                           for (int value : shard_ops) {
                               local_sum += value;
                           }
                           sum_->fetch_add(local_sum, std::memory_order_relaxed);
                       })
                       ? pending()
                       : failed();

        case state::finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    state state_{state::split};
    af::thread_group_ref logic_threads_;
    af::parallel_mode mode_{af::parallel_mode::non_empty_only};
    af::sharded_ops<int> ops_{4};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<int>, 4> *shard_hits_{nullptr};
    std::atomic<int> *sum_{nullptr};
};

class InstanceParallelFailureTask final : public af::runtime_task {
public:
    InstanceParallelFailureTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads, std::atomic<int> &completed,
                             std::atomic<std::uint32_t> &failures) {
        logic_threads_ = logic_threads;
        completed_ = &completed;
        failures_ = &failures;
        ops_ = af::sharded_ops<int>(4);
        ops_.shards[0] = {1};
        ops_.shards[1] = {2};
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split:
            state_ = state::finish;
            return owner().parallel_shards(
                       logic_threads_, ops_, af::parallel_mode::non_empty_only, this,
                       [](std::uint16_t shard, std::vector<int> &) { return shard != 1; })
                       ? pending()
                       : failed();

        case state::finish:
            failures_->store(last_parallel_failures(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    state state_{state::split};
    af::thread_group_ref logic_threads_;
    af::sharded_ops<int> ops_{4};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint32_t> *failures_{nullptr};
};

class InstanceEmptyParallelTask final : public af::runtime_task {
public:
    InstanceEmptyParallelTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads, std::atomic<int> &completed,
                             std::atomic<int> &unexpected_handler_runs) {
        logic_threads_ = logic_threads;
        completed_ = &completed;
        unexpected_handler_runs_ = &unexpected_handler_runs;
        ops_ = af::sharded_ops<int>(4);
        return schedule_to(logic_threads_.front());
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split:
            state_ = state::finish;
            return owner().parallel_shards(
                       logic_threads_, ops_, af::parallel_mode::non_empty_only, this,
                       [this](std::uint16_t, std::vector<int> &) {
                           unexpected_handler_runs_->fetch_add(1, std::memory_order_release);
                       })
                       ? pending()
                       : failed();

        case state::finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    state state_{state::split};
    af::thread_group_ref logic_threads_;
    af::sharded_ops<int> ops_{4};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *unexpected_handler_runs_{nullptr};
};

class InstanceParallelBeginTask final : public af::runtime_task {
public:
    InstanceParallelBeginTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref first_thread, std::atomic<int> &completed,
                             std::atomic<std::uint16_t> &ran_on) {
        first_thread_ = first_thread;
        completed_ = &completed;
        ran_on_ = &ran_on;
        ops_ = af::sharded_ops<int>(2);
        ops_.shards[1] = {42};
        return schedule_to(first_thread_);
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split:
            state_ = state::finish;
            return owner().parallel_shards(first_thread_, ops_, af::parallel_mode::non_empty_only,
                                           this,
                                           [this](std::uint16_t, std::vector<int> &) {
                                               ran_on_->store(af::runtime::current_thread_index(),
                                                              std::memory_order_release);
                                           })
                       ? pending()
                       : failed();

        case state::finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    state state_{state::split};
    af::thread_ref first_thread_;
    af::sharded_ops<int> ops_{2};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
};

} // namespace

TEST(RuntimeInstanceParallelShardTests, NonEmptyOnlySkipsEmptyShards) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    auto task = af::make_task<InstanceParallelShardTask>(runtime);
    ASSERT_TRUE(
        task->do_it(logic_threads, af::parallel_mode::non_empty_only, completed, shard_hits, sum));

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    EXPECT_EQ(shard_hits[0].load(), 1);
    EXPECT_EQ(shard_hits[1].load(), 0);
    EXPECT_EQ(shard_hits[2].load(), 1);
    EXPECT_EQ(shard_hits[3].load(), 0);
    EXPECT_EQ(sum.load(), 6);
}

TEST(RuntimeInstanceParallelShardTests, AllShardsRunsNoopShards) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    auto task = af::make_task<InstanceParallelShardTask>(runtime);
    ASSERT_TRUE(
        task->do_it(logic_threads, af::parallel_mode::all_shards, completed, shard_hits, sum));

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    for (const auto &hit : shard_hits) {
        EXPECT_EQ(hit.load(), 1);
    }
    EXPECT_EQ(sum.load(), 6);
}

TEST(RuntimeInstanceParallelShardTests, FailuresAreVisibleToOwner) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::atomic<std::uint32_t> failures{0};

    auto task = af::make_task<InstanceParallelFailureTask>(runtime);
    ASSERT_TRUE(task->do_it(logic_threads, completed, failures));

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    EXPECT_EQ(failures.load(std::memory_order_acquire), 1U);
}

TEST(RuntimeInstanceParallelShardTests, EmptyNonEmptyParallelResumesOwner) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::atomic<int> unexpected_handler_runs{0};

    auto task = af::make_task<InstanceEmptyParallelTask>(runtime);
    ASSERT_TRUE(task->do_it(logic_threads, completed, unexpected_handler_runs));

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    EXPECT_EQ(unexpected_handler_runs.load(std::memory_order_acquire), 0);
}

TEST(RuntimeInstanceParallelShardTests, ThreadBeginOverloadUsesAdjacentThreads) {
    af::runtime runtime(make_parallel_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{runtime.invalid_thread_index()};

    auto task = af::make_task<InstanceParallelBeginTask>(runtime);
    ASSERT_TRUE(task->do_it(logic_threads.front(), completed, ran_on));

    EXPECT_TRUE(wait_for_counter(completed, 1));
    runtime.stop();

    EXPECT_EQ(ran_on.load(std::memory_order_acquire), logic_threads.at(1).index);
}
