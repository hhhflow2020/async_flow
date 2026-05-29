#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "caf/batch_sequencer.hpp"
#include "caf/caf.hpp"
#include "caf/detail/bounded_queues.hpp"

namespace {

template <typename T>
bool wait_until_at_least(std::atomic<T>& value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

template <typename T>
bool wait_until_equal(std::atomic<T>& value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) != expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

enum class TestThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    enum_num_end,
};

struct TestRuntimeTraits {
    using Thread = TestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TestThread::enum_num_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr caf::QueueFullPolicy queue_full_policy = caf::QueueFullPolicy::Reject;
};

using Runtime = caf::AsyncRuntime<TestRuntimeTraits>;
using Task = Runtime::Task;

class RuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        Runtime::init();
    }

    void TearDown() override {
        Runtime::shutdown();
    }
};

class OneShotTask final : public Task {
public:
    bool do_it(TestThread target, std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(target);
    }

private:
    caf::TaskResult run() override {
        ran_on_->store(Runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class FailTask final : public Task {
public:
    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(TestThread::Logic_0);
    }

private:
    caf::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return failed();
    }

    std::atomic<int>* completed_{nullptr};
};

class HopTask final : public Task {
public:
    bool do_it(std::atomic<int>* completed, std::array<std::atomic<std::uint16_t>, 4>* seen) {
        completed_ = completed;
        seen_ = seen;
        state_ = State::Start;
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Start,
        Db,
        Logic,
        Finish,
    };

    caf::TaskResult run() override {
        switch (state_) {
        case State::Start:
            (*seen_)[0].store(Runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Db;
            return pending_on(TestThread::DB_0);

        case State::Db:
            (*seen_)[1].store(Runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Logic;
            return pending_on(TestThread::Logic_1);

        case State::Logic:
            (*seen_)[2].store(Runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Finish;
            return again();

        case State::Finish:
            (*seen_)[3].store(Runtime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Start};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 4>* seen_{nullptr};
};

class ParallelTask final : public Task {
public:
    bool do_it(
        caf::ParallelMode mode,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits,
        std::atomic<int>* sum) {
        mode_ = mode;
        completed_ = completed;
        shard_hits_ = shard_hits;
        sum_ = sum;

        ops_ = caf::ShardedOps<int>(4);
        ops_.shards[0] = {1};
        ops_.shards[2] = {2, 3};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    caf::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                TestThread::Logic_0,
                ops_,
                mode_,
                this,
                [this](std::uint16_t shard, std::vector<int>& shard_ops) {
                    (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
                    int local_sum = 0;
                    for (int value : shard_ops) {
                        local_sum += value;
                    }
                    sum_->fetch_add(local_sum, std::memory_order_relaxed);
                });
            return pending();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    caf::ParallelMode mode_{caf::ParallelMode::NonEmptyOnly};
    caf::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
    std::atomic<int>* sum_{nullptr};
};

class EmptyParallelTask final : public Task {
public:
    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        ops_ = caf::ShardedOps<int>(4);
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    caf::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                TestThread::Logic_0,
                ops_,
                caf::ParallelMode::NonEmptyOnly,
                this,
                [](std::uint16_t, std::vector<int>&) { FAIL() << "empty shards should skip"; });
            return pending();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    caf::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
};

class OrderedTask final : public Task {
public:
    bool do_it(
        std::uint64_t batch_id,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits,
        std::array<std::atomic<std::uint64_t>, 4>* batch_seen) {
        batch_id_ = batch_id;
        completed_ = completed;
        shard_hits_ = shard_hits;
        batch_seen_ = batch_seen;
        ops_ = caf::ShardedOps<int>(4);
        ops_.shards[0] = {7};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    caf::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            state_ = State::Finish;
            Runtime::parallel_shards_ordered(
                TestThread::Logic_0,
                ops_,
                batch_id_,
                this,
                [this](std::uint16_t shard, std::vector<int>&, std::uint64_t batch_id) {
                    (*shard_hits_)[shard].fetch_add(1, std::memory_order_relaxed);
                    (*batch_seen_)[shard].store(batch_id, std::memory_order_release);
                });
            return pending();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Apply};
    std::uint64_t batch_id_{0};
    caf::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
    std::array<std::atomic<std::uint64_t>, 4>* batch_seen_{nullptr};
};

enum class TinyThread : std::uint16_t {
    Logic_0,
    enum_num_end,
};

struct TinyRuntimeTraits {
    using Thread = TinyThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TinyThread::enum_num_end);
    static constexpr std::size_t spsc_queue_capacity = 2;
    static constexpr std::size_t external_queue_capacity = 2;
    static constexpr caf::QueueFullPolicy queue_full_policy = caf::QueueFullPolicy::Reject;
};

using TinyRuntime = caf::AsyncRuntime<TinyRuntimeTraits>;
using TinyTask = TinyRuntime::Task;

class BlockingTinyTask final : public TinyTask {
public:
    bool do_it(std::atomic<int>* started, std::atomic<bool>* release, std::atomic<int>* completed) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        return schedule(TinyThread::Logic_0);
    }

private:
    caf::TaskResult run() override {
        started_->fetch_add(1, std::memory_order_release);
        started_->notify_one();
        while (!release_->load(std::memory_order_acquire)) {
            release_->wait(false, std::memory_order_acquire);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* started_{nullptr};
    std::atomic<bool>* release_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class TinyNoopTask final : public TinyTask {
public:
    bool do_it(std::atomic<int>* completed, std::atomic<int>* destroyed) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TinyThread::Logic_0);
    }

    ~TinyNoopTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    caf::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* destroyed_{nullptr};
};

enum class YieldThread : std::uint16_t {
    Logic_0,
    Logic_1,
    enum_num_end,
};

struct YieldRuntimeTraits {
    using Thread = YieldThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(YieldThread::enum_num_end);
    static constexpr std::size_t spsc_queue_capacity = 64;
    static constexpr std::size_t external_queue_capacity = 64;
    static constexpr caf::QueueFullPolicy queue_full_policy = caf::QueueFullPolicy::Yield;
};

using YieldRuntime = caf::AsyncRuntime<YieldRuntimeTraits>;
using YieldTask = YieldRuntime::Task;

class YieldCountTask final : public YieldTask {
public:
    bool do_it(YieldThread thread, std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(thread);
    }

private:
    caf::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

} // namespace

TEST(QueueTests, BoundedSpscPreservesFifoAndRejectsWhenFull) {
    caf::detail::BoundedSpscQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
    EXPECT_TRUE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &c);
}

TEST(QueueTests, BoundedMpmcRejectsWhenFull) {
    caf::detail::BoundedMpmcQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
}

TEST(UtilityTests, SplitByShardGroupsByKey) {
    struct Op {
        std::uint64_t key;
        int value;
    };

    std::vector<Op> ops{{0, 10}, {1, 11}, {4, 14}, {6, 16}};
    auto sharded = Runtime::split_by_shard(std::move(ops), 4, [](const Op& op) {
        return op.key;
    });

    ASSERT_EQ(sharded.shard_count(), 4);
    ASSERT_EQ(sharded.shards[0].size(), 2);
    ASSERT_EQ(sharded.shards[1].size(), 1);
    ASSERT_EQ(sharded.shards[2].size(), 1);
    ASSERT_TRUE(sharded.shards[3].empty());
}

TEST(UtilityTests, BatchSequencerBuffersOutOfOrderBatches) {
    caf::BatchSequencer<int> sequencer(1);
    std::vector<int> submitted;

    auto submit = [&](int value) {
        submitted.push_back(value);
    };

    EXPECT_EQ(sequencer.submit(2, 20, submit), caf::BatchSubmitStatus::Buffered);
    EXPECT_TRUE(submitted.empty());
    EXPECT_EQ(sequencer.submit(1, 10, submit), caf::BatchSubmitStatus::Submitted);
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_EQ(submitted[0], 10);
    EXPECT_EQ(submitted[1], 20);
    EXPECT_EQ(sequencer.submit(1, 10, submit), caf::BatchSubmitStatus::Duplicate);
}

TEST_F(RuntimeFixture, OneShotTaskRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThread::Logic_2, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThread::Logic_2));
}

TEST_F(RuntimeFixture, FailedTaskIsReleased) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<FailTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, StateMachineCanHopThreadsAndAgainOnCurrentThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 4> seen{};
    for (auto& value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<HopTask>(&completed, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(), Runtime::thread_index(TestThread::Logic_0));
    EXPECT_EQ(seen[1].load(), Runtime::thread_index(TestThread::DB_0));
    EXPECT_EQ(seen[2].load(), Runtime::thread_index(TestThread::Logic_1));
    EXPECT_EQ(seen[3].load(), Runtime::thread_index(TestThread::Logic_1));
}

TEST_F(RuntimeFixture, ParallelShardsNonEmptyOnlySkipsEmptyShards) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    ASSERT_TRUE(Runtime::start_task<ParallelTask>(
        caf::ParallelMode::NonEmptyOnly,
        &completed,
        &shard_hits,
        &sum));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(shard_hits[0].load(), 1);
    EXPECT_EQ(shard_hits[1].load(), 0);
    EXPECT_EQ(shard_hits[2].load(), 1);
    EXPECT_EQ(shard_hits[3].load(), 0);
    EXPECT_EQ(sum.load(), 6);
}

TEST_F(RuntimeFixture, ParallelShardsAllShardsRunsNoopShards) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::atomic<int> sum{0};

    ASSERT_TRUE(Runtime::start_task<ParallelTask>(
        caf::ParallelMode::AllShards,
        &completed,
        &shard_hits,
        &sum));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    for (const auto& hit : shard_hits) {
        EXPECT_EQ(hit.load(), 1);
    }
    EXPECT_EQ(sum.load(), 6);
}

TEST_F(RuntimeFixture, EmptyNonEmptyParallelResumesOwner) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<EmptyParallelTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, OrderedBatchRunsEveryShardAndAcceptsContiguousBatches) {
    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::array<std::atomic<std::uint64_t>, 4> batch_seen{};

    ASSERT_TRUE(Runtime::start_task<OrderedTask>(1U, &completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 1);
        EXPECT_EQ(batch_seen[i].load(), 1U);
    }

    ASSERT_TRUE(Runtime::start_task<OrderedTask>(2U, &completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_until_at_least(completed, 2));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 2);
        EXPECT_EQ(batch_seen[i].load(), 2U);
    }
}

TEST(RuntimeBackpressureTests, RejectPolicyReturnsFalseAndDeletesRejectedTask) {
    TinyRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(TinyRuntime::start_task<BlockingTinyTask>(&started, &release, &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    EXPECT_TRUE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_TRUE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_FALSE(TinyRuntime::start_task<TinyNoopTask>(&completed, &destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);

    release.store(true, std::memory_order_release);
    release.notify_one();
    EXPECT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_TRUE(wait_until_at_least(destroyed, 3));

    TinyRuntime::shutdown();
}

TEST(RuntimeBackpressureTests, YieldPolicyAllowsManyExternalProducers) {
    YieldRuntime::init();

    constexpr int producer_count = 4;
    constexpr int tasks_per_producer = 200;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([producer, &completed, &all_started] {
            for (int i = 0; i < tasks_per_producer; ++i) {
                const YieldThread target =
                    ((producer + i) & 1) == 0 ? YieldThread::Logic_0 : YieldThread::Logic_1;
                if (!YieldRuntime::start_task<YieldCountTask>(target, &completed)) {
                    all_started.store(false, std::memory_order_release);
                    return;
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_TRUE(all_started.load(std::memory_order_acquire));
    EXPECT_TRUE(wait_until_at_least(completed, producer_count * tasks_per_producer));
    YieldRuntime::shutdown();
}
