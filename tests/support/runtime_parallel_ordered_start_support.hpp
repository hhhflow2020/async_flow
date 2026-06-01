#pragma once

struct OrderedStartStream {};
struct OrderedStartFailureStream {};

struct OrderedStartBatch {
    std::uint64_t batch_id{0};
    int value{0};
    std::vector<int> *applied{nullptr};
    std::atomic<int> *completed{nullptr};
};

class OrderedStartApplyTask final : public Task {
public:
    explicit OrderedStartApplyTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(OrderedStartBatch batch) {
        batch_ = batch;
        return schedule(TestThreads::Logic_0);
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
    std::vector<int> *applied{nullptr};
    std::atomic<int> *attempts{nullptr};
    std::atomic<int> *completed{nullptr};
    std::atomic<bool> *fail_first_start{nullptr};
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
        return schedule(TestThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        batch_.applied->push_back(batch_.value);
        batch_.completed->fetch_add(1, std::memory_order_release);
        return done();
    }

    OrderedStartFailureBatch batch_;
};
