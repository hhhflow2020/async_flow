#pragma once

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
