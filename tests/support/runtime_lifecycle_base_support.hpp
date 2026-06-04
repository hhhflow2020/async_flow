#pragma once

template <typename T> bool wait_until_at_least(std::atomic<T> &value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

struct TestLogicThreadTag;
struct TestDbThreadTag;

struct TestRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<TestLogicThreadTag, 4>(),
                                                      af::thread_group<TestDbThreadTag, 1>());
};

using Runtime = af::AsyncRuntime<TestRuntimeTraits>;
using Task = Runtime::Task;
using TestThread = Runtime::Thread;

struct TestThreads {
    static constexpr TestThread Logic_0 =
        Runtime::thread_group<TestLogicThreadTag>().template at<0>();
    static constexpr TestThread Logic_1 =
        Runtime::thread_group<TestLogicThreadTag>().template at<1>();
    static constexpr TestThread Logic_2 =
        Runtime::thread_group<TestLogicThreadTag>().template at<2>();
    static constexpr TestThread Logic_3 =
        Runtime::thread_group<TestLogicThreadTag>().template at<3>();
    static constexpr TestThread DB_0 = Runtime::thread_group<TestDbThreadTag>().template at<0>();
};

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

    bool do_it(TestThread target, std::atomic<int> *completed, std::atomic<std::uint16_t> *ran_on) {
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

    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
};

class ManualStartTask final : public Task {
public:
    explicit ManualStartTask(Task::FactoryToken token) : Task(token) {}

    bool begin_on(TestThread target, std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class TryMakeTask final : public Task {
public:
    TryMakeTask(Task::FactoryToken token, bool fail) : Task(token) {
        if (fail) {
            throw std::runtime_error("try make task failure");
        }
    }

    bool begin_on(TestThread target, std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class ScheduleModeStartTask final : public Task {
public:
    explicit ScheduleModeStartTask(Task::FactoryToken token) : Task(token) {}

    bool begin_on(TestThread target, af::ScheduleMode mode, std::atomic<int> *completed,
                  std::atomic<std::uint16_t> *ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(target, mode);
    }

    bool begin_fast_on(TestThread target, std::atomic<int> *completed,
                       std::atomic<std::uint16_t> *ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule_fast(target);
    }

    bool begin_ordered_on(TestThread target, std::atomic<int> *completed,
                          std::atomic<std::uint16_t> *ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule_ordered(target);
    }

private:
    af::TaskResult run() override {
        ran_on_->store(Runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
};

class RuntimeFastScheduleChildTask final : public Task {
public:
    explicit RuntimeFastScheduleChildTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *result,
               std::array<std::atomic<std::uint16_t>, 2> *seen) {
        completed_ = completed;
        result_ = result;
        seen_ = seen;
        return schedule(TestThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        (*seen_)[0].store(Runtime::current_thread_index(), std::memory_order_release);
        auto child = Runtime::make_task<ScheduleModeStartTask>();
        const bool ok = child->begin_fast_on(TestThreads::Logic_1, completed_, &(*seen_)[1]);
        if (!ok) {
            result_->store(-1, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        result_->store(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *result_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
};

class UnscheduledTask final : public Task {
public:
    UnscheduledTask(Task::FactoryToken token, std::atomic<int> *destroyed)
        : Task(token), destroyed_(destroyed) {}

    ~UnscheduledTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

    void configure_without_schedule() noexcept {}

private:
    af::TaskResult run() override {
        return failed();
    }

    std::atomic<int> *destroyed_{nullptr};
};

class TrackedDoneTask final : public Task {
public:
    TrackedDoneTask(Task::FactoryToken token, std::atomic<int> *destroyed)
        : Task(token), destroyed_(destroyed) {}

    ~TrackedDoneTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

    bool do_it(std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(TestThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *destroyed_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class FailTask final : public Task {
public:
    explicit FailTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(TestThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return failed();
    }

    std::atomic<int> *completed_{nullptr};
};

class CancelResultTask final : public Task {
public:
    explicit CancelResultTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *destroyed) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TestThreads::Logic_0);
    }

    ~CancelResultTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return cancelled();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

class HopTask final : public Task {
public:
    explicit HopTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *completed, std::array<std::atomic<std::uint16_t>, 4> *seen) {
        completed_ = completed;
        seen_ = seen;
        state_ = State::Start;
        return schedule(TestThreads::Logic_0);
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
            return pending_on(TestThreads::DB_0);

        case State::Db:
            (*seen_)[1].store(Runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Logic;
            return pending_on(TestThreads::Logic_1);

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
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 4> *seen_{nullptr};
};

class OrderedPendingModeTask final : public Task {
public:
    explicit OrderedPendingModeTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *completed, std::array<std::atomic<std::uint16_t>, 2> *seen) {
        completed_ = completed;
        seen_ = seen;
        return schedule(TestThreads::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Start,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            (*seen_)[0].store(Runtime::current_thread_index(), std::memory_order_release);
            state_ = State::Finish;
            return pending_ordered(TestThreads::Logic_1);

        case State::Finish:
            (*seen_)[1].store(Runtime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_{State::Start};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
};

class DelayedStartTask final : public Task {
public:
    explicit DelayedStartTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(TestThread target, std::chrono::milliseconds delay, std::atomic<int> *completed,
               std::atomic<std::uint16_t> *ran_on, std::atomic<std::int64_t> *elapsed_ms) {
        completed_ = completed;
        ran_on_ = ran_on;
        elapsed_ms_ = elapsed_ms;
        start_ = std::chrono::steady_clock::now();
        return schedule_after(target, delay);
    }

private:
    af::TaskResult run() override {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_);
        elapsed_ms_->store(elapsed.count(), std::memory_order_release);
        ran_on_->store(Runtime::current_thread_index(), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::chrono::steady_clock::time_point start_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
    std::atomic<std::int64_t> *elapsed_ms_{nullptr};
};

class DelayedPendingTask final : public Task {
public:
    explicit DelayedPendingTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::chrono::milliseconds delay, std::atomic<int> *completed,
               std::array<std::atomic<std::uint16_t>, 2> *seen,
               std::atomic<std::int64_t> *elapsed_ms) {
        delay_ = delay;
        completed_ = completed;
        seen_ = seen;
        elapsed_ms_ = elapsed_ms;
        return schedule(TestThreads::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Start,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Start:
            (*seen_)[0].store(Runtime::current_thread_index(), std::memory_order_release);
            start_ = std::chrono::steady_clock::now();
            state_ = State::Finish;
            return pending_after(TestThreads::DB_0, delay_);

        case State::Finish: {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_);
            elapsed_ms_->store(elapsed.count(), std::memory_order_release);
            (*seen_)[1].store(Runtime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        }

        return failed();
    }

    State state_{State::Start};
    std::chrono::milliseconds delay_{0};
    std::chrono::steady_clock::time_point start_{};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
    std::atomic<std::int64_t> *elapsed_ms_{nullptr};
};

class CountingServiceTask final : public af::detail::RuntimeServiceTask {
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
        idle_runs_.fetch_add(1, std::memory_order_relaxed);
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
    std::atomic<int> idle_runs_{0};
};

class ServiceControlTask final : public Task {
public:
    explicit ServiceControlTask(Task::FactoryToken token) : Task(token) {}

    enum class Operation : std::uint8_t {
        Register,
        Unregister,
        Barrier,
    };

    bool do_it(TestThread target, CountingServiceTask *service, Operation operation,
               std::atomic<int> *completed, std::atomic<bool> *ok) {
        target_ = target;
        service_ = service;
        operation_ = operation;
        completed_ = completed;
        ok_ = ok;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        bool ok = true;
        switch (operation_) {
        case Operation::Register:
            ok = Runtime::register_service_task(target_, service_);
            break;
        case Operation::Unregister:
            ok = Runtime::unregister_service_task(target_, service_);
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

    TestThread target_{TestThreads::Logic_0};
    CountingServiceTask *service_{nullptr};
    Operation operation_{Operation::Barrier};
    std::atomic<int> *completed_{nullptr};
    std::atomic<bool> *ok_{nullptr};
};

static_assert(!std::is_default_constructible_v<OneShotTask>);
static_assert(!std::is_constructible_v<UnscheduledTask, std::atomic<int> *>);
