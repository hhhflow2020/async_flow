#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

namespace af::test::runtime_parallel {

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

enum class TestThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    enum_thread_index_end,
};

struct TestRuntimeTraits {
    using Thread = TestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using Runtime = af::AsyncRuntime<TestRuntimeTraits>;
using Task = Runtime::Task;

class ParallelRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        Runtime::init();
    }

    void TearDown() override {
        Runtime::shutdown();
    }
};

class ParallelTask final : public Task {
public:
    explicit ParallelTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        af::ParallelMode mode,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits,
        std::atomic<int>* sum) {
        mode_ = mode;
        completed_ = completed;
        shard_hits_ = shard_hits;
        sum_ = sum;

        ops_ = af::ShardedOps<int>(4);
        ops_.shards[0] = {1};
        ops_.shards[2] = {2, 3};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
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
    af::ParallelMode mode_{af::ParallelMode::NonEmptyOnly};
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
    std::atomic<int>* sum_{nullptr};
};

class ParallelFailureTask final : public Task {
public:
    explicit ParallelFailureTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<std::uint32_t>* failures) {
        completed_ = completed;
        failures_ = failures;

        ops_ = af::ShardedOps<int>(4);
        ops_.shards[0] = {1};
        ops_.shards[1] = {2};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                TestThread::Logic_0,
                ops_,
                af::ParallelMode::NonEmptyOnly,
                this,
                [](std::uint16_t shard, std::vector<int>&) {
                    return shard != 1;
                });
            return pending();

        case State::Finish:
            failures_->store(last_parallel_failures(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint32_t>* failures_{nullptr};
};

class EmptyParallelTask final : public Task {
public:
    explicit EmptyParallelTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        ops_ = af::ShardedOps<int>(4);
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                TestThread::Logic_0,
                ops_,
                af::ParallelMode::NonEmptyOnly,
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
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
};

class OrderedTask final : public Task {
public:
    explicit OrderedTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        std::uint64_t batch_id,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits,
        std::array<std::atomic<std::uint64_t>, 4>* batch_seen) {
        batch_id_ = batch_id;
        completed_ = completed;
        shard_hits_ = shard_hits;
        batch_seen_ = batch_seen;
        ops_ = af::ShardedOps<int>(4);
        ops_.shards[0] = {7};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
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
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
    std::array<std::atomic<std::uint64_t>, 4>* batch_seen_{nullptr};
};

class DefaultParallelOverloadTask final : public Task {
public:
    explicit DefaultParallelOverloadTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        ops_ = af::ShardedOps<int>(2);
        ops_.shards[1] = {42};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                ops_,
                af::ParallelMode::NonEmptyOnly,
                this,
                [this](std::uint16_t, std::vector<int>&) {
                    ran_on_->store(Runtime::current_thread_index(), std::memory_order_release);
                });
            return pending();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    af::ShardedOps<int> ops_{2};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class OrderedOverloadTask final : public Task {
public:
    explicit OrderedOverloadTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        std::uint64_t batch_id,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits) {
        batch_id_ = batch_id;
        completed_ = completed;
        shard_hits_ = shard_hits;
        ops_ = af::ShardedOps<int>(4);
        ops_.shards[0] = {1};
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            state_ = State::Finish;
            Runtime::parallel_shards(
                ops_,
                af::ParallelMode::AllShards,
                batch_id_,
                this,
                [this](std::uint16_t shard, std::vector<int>&, std::uint64_t) {
                    (*shard_hits_)[shard].fetch_add(1, std::memory_order_release);
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
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
};

class OrderedFailureTask final : public Task {
public:
    explicit OrderedFailureTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        std::uint64_t batch_id,
        std::uint16_t fail_shard,
        std::atomic<int>* completed,
        std::atomic<std::uint32_t>* failures) {
        batch_id_ = batch_id;
        fail_shard_ = fail_shard;
        completed_ = completed;
        failures_ = failures;
        ops_ = af::ShardedOps<int>(4);
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            state_ = State::Finish;
            Runtime::parallel_shards_ordered(
                TestThread::Logic_0,
                ops_,
                batch_id_,
                this,
                [this](std::uint16_t shard, std::vector<int>&, std::uint64_t) {
                    return shard != fail_shard_;
                });
            return pending();

        case State::Finish:
            failures_->store(last_parallel_failures(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Apply};
    std::uint64_t batch_id_{0};
    std::uint16_t fail_shard_{0};
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint32_t>* failures_{nullptr};
};

class OrderedRetryableTask final : public Task {
public:
    explicit OrderedRetryableTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(
        std::uint64_t batch_id,
        std::atomic<int>* completed,
        std::array<std::atomic<int>, 4>* shard_hits,
        std::atomic<std::uint32_t>* failures) {
        batch_id_ = batch_id;
        completed_ = completed;
        shard_hits_ = shard_hits;
        failures_ = failures;
        ops_ = af::ShardedOps<int>(4);
        return schedule(TestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Apply,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Apply:
            state_ = State::Finish;
            Runtime::parallel_shards_ordered(
                TestThread::Logic_0,
                ops_,
                batch_id_,
                af::retryable_ordered_batch_options,
                this,
                [this](std::uint16_t shard, std::vector<int>&, std::uint64_t) {
                    (*shard_hits_)[shard].fetch_add(1, std::memory_order_release);
                });
            return pending();

        case State::Finish:
            failures_->store(last_parallel_failures(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    State state_{State::Apply};
    std::uint64_t batch_id_{0};
    af::ShardedOps<int> ops_{4};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<int>, 4>* shard_hits_{nullptr};
    std::atomic<std::uint32_t>* failures_{nullptr};
};

struct OrderedStartStream {};
struct OrderedStartFailureStream {};

struct OrderedStartBatch {
    std::uint64_t batch_id{0};
    int value{0};
    std::vector<int>* applied{nullptr};
    std::atomic<int>* completed{nullptr};
};

class OrderedStartApplyTask final : public Task {
public:
    explicit OrderedStartApplyTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(OrderedStartBatch batch) {
        batch_ = batch;
        return schedule(TestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        batch_.applied->push_back(batch_.value);
        batch_.completed->fetch_add(1, std::memory_order_release);
        return done();
    }

    OrderedStartBatch batch_;
};

struct OrderedStartFailureBatch {
    std::uint64_t batch_id{0};
    int value{0};
    std::vector<int>* applied{nullptr};
    std::atomic<int>* attempts{nullptr};
    std::atomic<int>* completed{nullptr};
    std::atomic<bool>* fail_first_start{nullptr};
};

class OrderedStartFailingApplyTask final : public Task {
public:
    explicit OrderedStartFailingApplyTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(OrderedStartFailureBatch batch) {
        batch.attempts->fetch_add(1, std::memory_order_release);
        if (batch.batch_id == 1U &&
            !batch.fail_first_start->exchange(true, std::memory_order_acq_rel)) {
            return false;
        }

        batch_ = batch;
        return schedule(TestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        batch_.applied->push_back(batch_.value);
        batch_.completed->fetch_add(1, std::memory_order_release);
        return done();
    }

    OrderedStartFailureBatch batch_;
};

} // namespace af::test::runtime_parallel
