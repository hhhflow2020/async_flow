#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

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
    explicit OneShotTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(TestThread target, std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        ran_on_->store(Runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class ManualStartTask final : public Task {
public:
    explicit ManualStartTask(Task::FactoryToken token) : Task(token) {}

    bool begin_on(TestThread target, std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

class UnscheduledTask final : public Task {
public:
    UnscheduledTask(Task::FactoryToken token, std::atomic<int>* destroyed)
        : Task(token), destroyed_(destroyed) {}

    ~UnscheduledTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

    void configure_without_schedule() noexcept {}

private:
    af::TaskResult run() override {
        return failed();
    }

    std::atomic<int>* destroyed_{nullptr};
};

class TrackedDoneTask final : public Task {
public:
    TrackedDoneTask(Task::FactoryToken token, std::atomic<int>* destroyed)
        : Task(token), destroyed_(destroyed) {}

    ~TrackedDoneTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(TestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* destroyed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class FailTask final : public Task {
public:
    explicit FailTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(TestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return failed();
    }

    std::atomic<int>* completed_{nullptr};
};

class CancelResultTask final : public Task {
public:
    explicit CancelResultTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* destroyed) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TestThread::Logic_0);
    }

    ~CancelResultTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return cancelled();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* destroyed_{nullptr};
};

class HopTask final : public Task {
public:
    explicit HopTask(Task::FactoryToken token) : Task(token) {}

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

    af::TaskResult run() override {
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

enum class TinyThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    enum_thread_index_end,
};

struct TinyRuntimeTraits {
    using Thread = TinyThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TinyThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 2;
    static constexpr std::size_t external_queue_capacity = 2;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using TinyRuntime = af::AsyncRuntime<TinyRuntimeTraits>;
using TinyTask = TinyRuntime::Task;

class BlockingTinyTask final : public TinyTask {
public:
    explicit BlockingTinyTask(TinyTask::FactoryToken token) : TinyTask(token) {}

    bool do_it(std::atomic<int>* started, std::atomic<bool>* release, std::atomic<int>* completed) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        return schedule(TinyThread::Logic_0);
    }

private:
    af::TaskResult run() override {
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
    explicit TinyNoopTask(TinyTask::FactoryToken token) : TinyTask(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* destroyed) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TinyThread::Logic_0);
    }

    ~TinyNoopTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* destroyed_{nullptr};
};

enum class YieldThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    enum_thread_index_end,
};

struct YieldRuntimeTraits {
    using Thread = YieldThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(YieldThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 64;
    static constexpr std::size_t external_queue_capacity = 64;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using YieldRuntime = af::AsyncRuntime<YieldRuntimeTraits>;
using YieldTask = YieldRuntime::Task;

class YieldCountTask final : public YieldTask {
public:
    explicit YieldCountTask(YieldTask::FactoryToken token) : YieldTask(token) {}

    bool do_it(YieldThread thread, std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(thread);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

class YieldFanoutTask final : public YieldTask {
public:
    explicit YieldFanoutTask(YieldTask::FactoryToken token) : YieldTask(token) {}

    bool do_it(int child_count, std::atomic<int>* completed, std::atomic<bool>* all_started) {
        child_count_ = child_count;
        completed_ = completed;
        all_started_ = all_started;
        return schedule(YieldThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        for (int i = 0; i < child_count_; ++i) {
            if (!YieldRuntime::start_task<YieldCountTask>(YieldThread::Logic_0, completed_)) {
                all_started_->store(false, std::memory_order_release);
                return failed();
            }
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int child_count_{0};
    std::atomic<int>* completed_{nullptr};
    std::atomic<bool>* all_started_{nullptr};
};

enum class NoInitThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    enum_thread_index_end,
};

struct NoInitRuntimeTraits {
    using Thread = NoInitThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(NoInitThread::enum_thread_index_end);
};

using NoInitRuntime = af::AsyncRuntime<NoInitRuntimeTraits>;
using NoInitTaskBase = NoInitRuntime::Task;

class NoInitTask final : public NoInitTaskBase {
public:
    explicit NoInitTask(NoInitTaskBase::FactoryToken token) : NoInitTaskBase(token) {}

    bool do_it(std::atomic<int>* destroyed) {
        destroyed_ = destroyed;
        return schedule(NoInitThread::Logic_0);
    }

    ~NoInitTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::TaskResult run() override {
        return failed();
    }

    std::atomic<int>* destroyed_{nullptr};
};

enum class WaitShutdownThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    DB_0,
    enum_thread_index_end,
};

struct WaitShutdownRuntimeTraits {
    using Thread = WaitShutdownThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(WaitShutdownThread::enum_thread_index_end);
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using WaitShutdownRuntime = af::AsyncRuntime<WaitShutdownRuntimeTraits>;
using WaitShutdownTaskBase = WaitShutdownRuntime::Task;

class WaitShutdownBlockingTask final : public WaitShutdownTaskBase {
public:
    explicit WaitShutdownBlockingTask(WaitShutdownTaskBase::FactoryToken token)
        : WaitShutdownTaskBase(token) {}

    bool do_it(std::atomic<int>* started, std::atomic<bool>* release, std::atomic<int>* completed) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        return schedule(WaitShutdownThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        started_->fetch_add(1, std::memory_order_release);
        started_->notify_one();
        while (!release_->load(std::memory_order_acquire)) {
            release_->wait(false, std::memory_order_acquire);
        }
        completed_->fetch_add(1, std::memory_order_release);
        completed_->notify_one();
        return done();
    }

    std::atomic<int>* started_{nullptr};
    std::atomic<bool>* release_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class WaitShutdownHopDuringStopTask final : public WaitShutdownTaskBase {
public:
    explicit WaitShutdownHopDuringStopTask(WaitShutdownTaskBase::FactoryToken token)
        : WaitShutdownTaskBase(token) {}

    bool do_it(
        std::atomic<int>* started,
        std::atomic<bool>* release,
        std::atomic<int>* completed,
        std::array<std::atomic<std::uint16_t>, 2>* seen) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        seen_ = seen;
        return schedule(WaitShutdownThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Start,
        Db,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            (*seen_)[0].store(WaitShutdownRuntime::current_thread_index(), std::memory_order_release);
            started_->fetch_add(1, std::memory_order_release);
            started_->notify_one();
            while (!release_->load(std::memory_order_acquire)) {
                release_->wait(false, std::memory_order_acquire);
            }
            state_ = State::Db;
            return pending_on(WaitShutdownThread::DB_0);

        case State::Db:
            (*seen_)[1].store(WaitShutdownRuntime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            completed_->notify_one();
            return done();
        }

        return failed();
    }

    State state_{State::Start};
    std::atomic<int>* started_{nullptr};
    std::atomic<bool>* release_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2>* seen_{nullptr};
};

class WaitShutdownRejectedTask final : public WaitShutdownTaskBase {
public:
    explicit WaitShutdownRejectedTask(WaitShutdownTaskBase::FactoryToken token)
        : WaitShutdownTaskBase(token) {}

    ~WaitShutdownRejectedTask() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

    bool do_it(std::atomic<int>* destroyed) {
        destroyed_ = destroyed;
        return schedule(WaitShutdownThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        return failed();
    }

    std::atomic<int>* destroyed_{nullptr};
};

enum class FastShutdownThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    enum_thread_index_end,
};

struct FastShutdownRuntimeTraits {
    using Thread = FastShutdownThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FastShutdownThread::enum_thread_index_end);
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
};

using FastShutdownRuntime = af::AsyncRuntime<FastShutdownRuntimeTraits>;
using FastShutdownTaskBase = FastShutdownRuntime::Task;

class FastShutdownPendingTask final : public FastShutdownTaskBase {
public:
    explicit FastShutdownPendingTask(FastShutdownTaskBase::FactoryToken token)
        : FastShutdownTaskBase(token) {}

    bool do_it(std::atomic<int>* entered) {
        entered_ = entered;
        return schedule(FastShutdownThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        entered_->fetch_add(1, std::memory_order_release);
        entered_->notify_one();
        return pending();
    }

    std::atomic<int>* entered_{nullptr};
};

static_assert(!std::is_default_constructible_v<OneShotTask>);
static_assert(!std::is_constructible_v<UnscheduledTask, std::atomic<int>*>);
static_assert(!std::is_default_constructible_v<NoInitTask>);

} // namespace

TEST_F(RuntimeFixture, OneShotTaskRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThread::Logic_2, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThread::Logic_2));
}

TEST_F(RuntimeFixture, MakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = Runtime::make_task<ManualStartTask>();
    ASSERT_TRUE(task);
    EXPECT_FALSE(task.scheduled());
    ASSERT_TRUE(task->begin_on(TestThread::Logic_3, &completed));
    EXPECT_TRUE(task.scheduled());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, UnscheduledCreatedTaskIsDestroyedByHandle) {
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<UnscheduledTask>(&destroyed);
        ASSERT_TRUE(task);
        task->configure_without_schedule();
        EXPECT_FALSE(task.scheduled());
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, CreatedHandleKeepsCompletedTaskAliveUntilReset) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<TrackedDoneTask>(&destroyed);
        ASSERT_TRUE(task->do_it(&completed));
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        EXPECT_EQ(destroyed.load(std::memory_order_acquire), 0);
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, FailedTaskIsReleased) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<FailTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, CancelledTaskIsReleased) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(Runtime::start_task<CancelResultTask>(&completed, &destroyed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(wait_until_at_least(destroyed, 1));
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

TEST_F(RuntimeFixture, WaitForIdleReturnsAfterAcceptedTasksComplete) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThread::Logic_1, &completed, &ran_on));
    Runtime::wait_for_idle();

    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThread::Logic_1));
    EXPECT_EQ(Runtime::unfinished_task_count(), 0U);
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
    EXPECT_EQ(TinyRuntime::unfinished_task_count(), 3U);

    release.store(true, std::memory_order_release);
    release.notify_one();
    EXPECT_TRUE(wait_until_at_least(completed, 3));
    EXPECT_TRUE(wait_until_at_least(destroyed, 3));
    EXPECT_EQ(TinyRuntime::unfinished_task_count(), 0U);

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

TEST(RuntimeBackpressureTests, YieldPolicyHandlesSameThreadFanoutWithBoundedLocalQueue) {
    YieldRuntime::init();

    constexpr int child_count = 128;
    std::atomic<int> completed{0};
    std::atomic<bool> all_started{true};

    ASSERT_TRUE(YieldRuntime::start_task<YieldFanoutTask>(child_count, &completed, &all_started));
    EXPECT_TRUE(wait_until_at_least(completed, child_count + 1));
    EXPECT_TRUE(all_started.load(std::memory_order_acquire));

    YieldRuntime::shutdown();
}

TEST(RuntimeShutdownTests, StartTaskFailsAndDestroysTaskWhenRuntimeIsNotInitialized) {
    NoInitRuntime::shutdown();

    std::atomic<int> destroyed{0};
    EXPECT_FALSE(NoInitRuntime::start_task<NoInitTask>(&destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, MakeTaskHandleDestroysTaskWhenScheduleFailsBeforeInit) {
    NoInitRuntime::shutdown();

    std::atomic<int> destroyed{0};
    {
        auto task = NoInitRuntime::make_task<NoInitTask>();
        EXPECT_FALSE(task->do_it(&destroyed));
        EXPECT_FALSE(task.scheduled());
    }
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyBlocksUntilAcceptedTasksComplete) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownBlockingTask>(
        &started,
        &release,
        &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyAllowsRuntimeThreadRescheduleWhileStopping) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto& value : seen) {
        value.store(WaitShutdownRuntime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownHopDuringStopTask>(
        &started,
        &release,
        &completed,
        &seen));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(seen[0].load(std::memory_order_acquire),
              WaitShutdownRuntime::thread_index(WaitShutdownThread::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire),
              WaitShutdownRuntime::thread_index(WaitShutdownThread::DB_0));
    EXPECT_EQ(WaitShutdownRuntime::unfinished_task_count(), 0U);
}

TEST(RuntimeShutdownTests, WaitForTasksPolicyRejectsExternalStartsWhileStopping) {
    WaitShutdownRuntime::init();

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(WaitShutdownRuntime::start_task<WaitShutdownBlockingTask>(
        &started,
        &release,
        &completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        WaitShutdownRuntime::shutdown();
        shutdown_done.store(true, std::memory_order_release);
        shutdown_done.notify_one();
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool saw_stopping = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (WaitShutdownRuntime::is_stopping()) {
            saw_stopping = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!saw_stopping) {
        release.store(true, std::memory_order_release);
        release.notify_one();
        shutdown_thread.join();
        FAIL() << "runtime did not enter stopping";
    }

    EXPECT_FALSE(WaitShutdownRuntime::start_task<WaitShutdownRejectedTask>(&destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    release.notify_one();
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, StopImmediatelyPolicyDoesNotWaitForPendingTasks) {
    FastShutdownRuntime::init();

    std::atomic<int> entered{0};
    ASSERT_TRUE(FastShutdownRuntime::start_task<FastShutdownPendingTask>(&entered));
    ASSERT_TRUE(wait_until_at_least(entered, 1));

    FastShutdownRuntime::shutdown();
    EXPECT_EQ(FastShutdownRuntime::unfinished_task_count(), 0U);
}
