#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_lifecycle_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", 4),
        af::cpu_threads("db", 1),
    };
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

class RuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(runtime_.start());
        logic_ = runtime_.thread_group("logic");
        db_ = runtime_.thread_group("db");
        ASSERT_EQ(logic_.size(), 4U);
        ASSERT_EQ(db_.size(), 1U);
    }

    void TearDown() override {
        runtime_.stop();
    }

    [[nodiscard]] af::thread_ref logic(std::size_t index) const noexcept {
        return logic_.at(index);
    }

    [[nodiscard]] af::thread_ref db() const noexcept {
        return db_.front();
    }

    template <typename TaskT, typename... Args> [[nodiscard]] bool start_task(Args &&...args) {
        auto task = af::make_task<TaskT>(runtime_);
        if (!task->do_it(std::forward<Args>(args)...)) {
            return false;
        }
        return true;
    }

    af::runtime runtime_{make_lifecycle_runtime_config()};
    af::thread_group_ref logic_{};
    af::thread_group_ref db_{};
};

class OneShotTask final : public af::runtime_task {
public:
    OneShotTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref target, std::atomic<int> &completed,
                             std::atomic<std::uint16_t> &ran_on) noexcept {
        completed_ = &completed;
        ran_on_ = &ran_on;
        return schedule_to(target);
    }

private:
    af::task_result run_task() noexcept override {
        ran_on_->store(af::runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
};

class PendingToTask final : public af::runtime_task {
public:
    PendingToTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref first, af::thread_ref second,
                             std::atomic<int> &completed,
                             std::array<std::atomic<std::uint16_t>, 2> &seen) noexcept {
        first_ = first;
        second_ = second;
        completed_ = &completed;
        seen_ = &seen;
        return schedule_to(first_);
    }

private:
    af::task_result run_task() noexcept override {
        if (!resumed_) {
            (*seen_)[0].store(af::runtime::current_thread_index(), std::memory_order_release);
            resumed_ = true;
            return pending_to(second_);
        }

        (*seen_)[1].store(af::runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::thread_ref first_{};
    af::thread_ref second_{};
    bool resumed_{false};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
};

class ManualStartTask final : public af::runtime_task {
public:
    ManualStartTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool begin_on(af::thread_ref target, std::atomic<int> &completed) noexcept {
        completed_ = &completed;
        return schedule_to(target);
    }

private:
    af::task_result run_task() noexcept override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class TryMakeTask final : public af::runtime_task {
public:
    TryMakeTask(factory_token token, af::runtime &owner, bool fail) : runtime_task(token, owner) {
        if (fail) {
            throw std::runtime_error("try make task failure");
        }
    }

    [[nodiscard]] bool begin_on(af::thread_ref target, std::atomic<int> &completed) noexcept {
        completed_ = &completed;
        return schedule_to(target);
    }

private:
    af::task_result run_task() noexcept override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class UnscheduledTask final : public af::runtime_task {
public:
    UnscheduledTask(factory_token token, af::runtime &owner, std::atomic<int> &destroyed)
        : runtime_task(token, owner), destroyed_(&destroyed) {}

    ~UnscheduledTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

    [[nodiscard]] bool do_it(af::thread_ref target) noexcept {
        return schedule_to(target);
    }

private:
    af::task_result run_task() noexcept override {
        return failed();
    }

    std::atomic<int> *destroyed_{nullptr};
};

class TrackedDoneTask final : public af::runtime_task {
public:
    TrackedDoneTask(factory_token token, af::runtime &owner, std::atomic<int> &destroyed)
        : runtime_task(token, owner), destroyed_(&destroyed) {}

    [[nodiscard]] bool do_it(af::thread_ref target, std::atomic<int> &completed) noexcept {
        completed_ = &completed;
        return schedule_to(target);
    }

    ~TrackedDoneTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::task_result run_task() noexcept override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *destroyed_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class ResultTask final : public af::runtime_task {
public:
    enum class Result : std::uint8_t {
        Done,
        Failed,
        Cancelled,
    };

    ResultTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref target, Result result, std::atomic<int> &completed,
                             std::atomic<int> *destroyed = nullptr) noexcept {
        result_ = result;
        completed_ = &completed;
        destroyed_ = destroyed;
        return schedule_to(target);
    }

    ~ResultTask() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::task_result run_task() noexcept override {
        completed_->fetch_add(1, std::memory_order_release);
        switch (result_) {
        case Result::Done:
            return done();
        case Result::Failed:
            return failed();
        case Result::Cancelled:
            return cancel();
        }
        return failed();
    }

    Result result_{Result::Done};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

class HopTask final : public af::runtime_task {
public:
    HopTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref logic0, af::thread_ref db, af::thread_ref logic1,
                             std::atomic<int> &completed,
                             std::array<std::atomic<std::uint16_t>, 4> &seen) noexcept {
        logic0_ = logic0;
        db_ = db;
        logic1_ = logic1;
        completed_ = &completed;
        seen_ = &seen;
        return schedule_to(logic0_);
    }

private:
    enum class State : std::uint8_t {
        Start,
        Db,
        Logic,
        Finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case State::Start:
            (*seen_)[0].store(af::runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Db;
            return pending_to(db_);

        case State::Db:
            (*seen_)[1].store(af::runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Logic;
            return pending_to(logic1_);

        case State::Logic:
            (*seen_)[2].store(af::runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Finish;
            return reschedule();

        case State::Finish:
            (*seen_)[3].store(af::runtime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    af::thread_ref logic0_{};
    af::thread_ref db_{};
    af::thread_ref logic1_{};
    State state_{State::Start};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 4> *seen_{nullptr};
};

class DelayedStartTask final : public af::runtime_task {
public:
    DelayedStartTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref target, std::chrono::milliseconds delay,
                             std::atomic<int> &completed, std::atomic<std::uint16_t> &ran_on,
                             std::atomic<std::int64_t> &elapsed_ms) noexcept {
        completed_ = &completed;
        ran_on_ = &ran_on;
        elapsed_ms_ = &elapsed_ms;
        start_ = std::chrono::steady_clock::now();
        return schedule_after(target, delay);
    }

private:
    af::task_result run_task() noexcept override {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_);
        elapsed_ms_->store(elapsed.count(), std::memory_order_release);
        ran_on_->store(af::runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::chrono::steady_clock::time_point start_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
    std::atomic<std::int64_t> *elapsed_ms_{nullptr};
};

class DelayedPendingTask final : public af::runtime_task {
public:
    DelayedPendingTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref first, af::thread_ref second,
                             std::chrono::milliseconds delay, std::atomic<int> &completed,
                             std::array<std::atomic<std::uint16_t>, 2> &seen,
                             std::atomic<std::int64_t> &elapsed_ms) noexcept {
        first_ = first;
        second_ = second;
        delay_ = delay;
        completed_ = &completed;
        seen_ = &seen;
        elapsed_ms_ = &elapsed_ms;
        return schedule_to(first_);
    }

private:
    af::task_result run_task() noexcept override {
        if (!resumed_) {
            (*seen_)[0].store(af::runtime::current_thread_index(), std::memory_order_release);
            start_ = std::chrono::steady_clock::now();
            resumed_ = true;
            return pending_after(second_, delay_);
        }

        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_);
        elapsed_ms_->store(elapsed.count(), std::memory_order_release);
        (*seen_)[1].store(af::runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::thread_ref first_{};
    af::thread_ref second_{};
    bool resumed_{false};
    std::chrono::milliseconds delay_{0};
    std::chrono::steady_clock::time_point start_{};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
    std::atomic<std::int64_t> *elapsed_ms_{nullptr};
};

class CountingServiceTask final : public af::detail::runtime_service_task {
public:
    void add_work(int count) noexcept {
        pending_.fetch_add(count, std::memory_order_release);
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        const int limit = static_cast<int>(budget == 0U ? 1U : budget);
        int pending = pending_.load(std::memory_order_acquire);
        while (pending > 0) {
            const int consumed = pending < limit ? pending : limit;
            if (pending_.compare_exchange_weak(pending, pending - consumed,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
                runs_.fetch_add(1, std::memory_order_release);
                consumed_.fetch_add(consumed, std::memory_order_release);
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] int consumed() const noexcept {
        return consumed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] int runs() const noexcept {
        return runs_.load(std::memory_order_acquire);
    }

private:
    std::atomic<int> pending_{0};
    std::atomic<int> consumed_{0};
    std::atomic<int> runs_{0};
};

class ServiceControlTask final : public af::runtime_task {
public:
    enum class Operation : std::uint8_t {
        Register,
        Unregister,
        Barrier,
    };

    ServiceControlTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref target, CountingServiceTask &service,
                             Operation operation, std::atomic<int> &completed,
                             std::atomic<bool> *ok) noexcept {
        target_ = target;
        service_ = &service;
        operation_ = operation;
        completed_ = &completed;
        ok_ = ok;
        return schedule_to(target_);
    }

private:
    af::task_result run_task() noexcept override {
        bool ok = true;
        switch (operation_) {
        case Operation::Register:
            ok = owner().register_service_task(target_.index, service_);
            break;
        case Operation::Unregister:
            ok = owner().unregister_service_task(target_.index, service_);
            break;
        case Operation::Barrier:
            break;
        }
        if (ok_ != nullptr) {
            ok_->store(ok, std::memory_order_release);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::thread_ref target_{};
    CountingServiceTask *service_{nullptr};
    Operation operation_{Operation::Barrier};
    std::atomic<int> *completed_{nullptr};
    std::atomic<bool> *ok_{nullptr};
};

[[nodiscard]] bool wait_until_service_consumed(const CountingServiceTask &service, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (service.consumed() < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

static_assert(!std::is_default_constructible_v<OneShotTask>);
static_assert(!std::is_constructible_v<UnscheduledTask, std::atomic<int> &>);

TEST_F(RuntimeFixture, OneShotTaskRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{runtime_.invalid_thread_index()};

    ASSERT_TRUE(start_task<OneShotTask>(logic(2), completed, ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), logic(2).index);
}

TEST_F(RuntimeFixture, PendingToAliasResumesOnRequestedThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto &value : seen) {
        value.store(runtime_.invalid_thread_index(), std::memory_order_relaxed);
    }

    ASSERT_TRUE(start_task<PendingToTask>(logic(0), logic(1), completed, seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), logic(0).index);
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), logic(1).index);
}

TEST_F(RuntimeFixture, MakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = af::make_task<ManualStartTask>(runtime_);
    ASSERT_TRUE(task);
    ASSERT_TRUE(task->begin_on(logic(3), completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, TryMakeTaskReturnsNullWhenConstructorThrows) {
    auto failed = af::try_make_task<TryMakeTask>(runtime_, true);
    EXPECT_FALSE(failed);
}

TEST_F(RuntimeFixture, TryMakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = af::try_make_task<TryMakeTask>(runtime_, false);
    ASSERT_TRUE(task);
    ASSERT_TRUE(task->begin_on(logic(3), completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, DelayedStartRunsOnRequestedThreadAfterDelay) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{runtime_.invalid_thread_index()};
    std::atomic<std::int64_t> elapsed_ms{0};

    ASSERT_TRUE(start_task<DelayedStartTask>(logic(2), std::chrono::milliseconds(20), completed,
                                             ran_on, elapsed_ms));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(ran_on.load(std::memory_order_acquire), logic(2).index);
    EXPECT_GE(elapsed_ms.load(std::memory_order_acquire), 15);
}

TEST_F(RuntimeFixture, PendingAfterResumesOnRequestedThreadAfterDelay) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    std::atomic<std::int64_t> elapsed_ms{0};
    for (auto &value : seen) {
        value.store(runtime_.invalid_thread_index(), std::memory_order_relaxed);
    }

    ASSERT_TRUE(start_task<DelayedPendingTask>(logic(0), db(), std::chrono::milliseconds(20),
                                               completed, seen, elapsed_ms));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(seen[0].load(std::memory_order_acquire), logic(0).index);
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), db().index);
    EXPECT_GE(elapsed_ms.load(std::memory_order_acquire), 15);
}

TEST_F(RuntimeFixture, ServiceTaskRunsWhenExecutorIsWoken) {
    CountingServiceTask service;
    std::atomic<int> completed{0};
    std::atomic<bool> ok{false};

    ASSERT_TRUE(start_task<ServiceControlTask>(
        logic(0), service, ServiceControlTask::Operation::Register, completed, &ok));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(ok.load(std::memory_order_acquire));

    service.add_work(3);
    ASSERT_TRUE(runtime_.wake_service_tasks(logic(0).index));
    ASSERT_TRUE(wait_until_service_consumed(service, 3));
    EXPECT_GE(service.runs(), 1);

    completed.store(0, std::memory_order_release);
    ok.store(false, std::memory_order_release);
    ASSERT_TRUE(start_task<ServiceControlTask>(
        logic(0), service, ServiceControlTask::Operation::Unregister, completed, &ok));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(ok.load(std::memory_order_acquire));

    const int consumed_after_unregister = service.consumed();
    service.add_work(2);
    ASSERT_TRUE(runtime_.wake_service_tasks(logic(0).index));

    completed.store(0, std::memory_order_release);
    ASSERT_TRUE(start_task<ServiceControlTask>(
        logic(0), service, ServiceControlTask::Operation::Barrier, completed, nullptr));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(service.consumed(), consumed_after_unregister);
}

TEST_F(RuntimeFixture, CreatedTaskIsDestroyedWhenInitialScheduleFails) {
    std::atomic<int> destroyed{0};

    auto *task = af::make_task<UnscheduledTask>(runtime_, destroyed);
    ASSERT_NE(task, nullptr);
    EXPECT_FALSE(task->do_it(af::thread_ref{}));

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, CompletedTaskIsDestroyedAfterRun) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    auto *task = af::make_task<TrackedDoneTask>(runtime_, destroyed);
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(task->do_it(logic(0), completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_TRUE(wait_until_at_least(destroyed, 1));
}

TEST_F(RuntimeFixture, FailedTaskIsReleased) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(start_task<ResultTask>(logic(0), ResultTask::Result::Failed, completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, CancelledTaskIsReleased) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(
        start_task<ResultTask>(logic(0), ResultTask::Result::Cancelled, completed, &destroyed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(wait_until_at_least(destroyed, 1));
}

TEST_F(RuntimeFixture, StateMachineCanHopThreadsAndAgainOnCurrentThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 4> seen{};
    for (auto &value : seen) {
        value.store(runtime_.invalid_thread_index(), std::memory_order_relaxed);
    }

    ASSERT_TRUE(start_task<HopTask>(logic(0), db(), logic(1), completed, seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), logic(0).index);
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), db().index);
    EXPECT_EQ(seen[2].load(std::memory_order_acquire), logic(1).index);
    EXPECT_EQ(seen[3].load(std::memory_order_acquire), logic(1).index);
}

TEST_F(RuntimeFixture, PostedWorkRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{runtime_.invalid_thread_index()};

    ASSERT_TRUE(runtime_.post(logic(1), [&] {
        ran_on.store(af::runtime::current_thread_index(), std::memory_order_release);
        completed.fetch_add(1, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), logic(1).index);
}

} // namespace
