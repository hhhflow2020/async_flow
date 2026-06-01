#if !defined(AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_parallel_shard_tasks_support.hpp is a runtime parallel test support detail"
#endif

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
