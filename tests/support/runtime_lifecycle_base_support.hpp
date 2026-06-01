#if !defined(AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_lifecycle_base_support.hpp is a runtime lifecycle test support detail"
#endif

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

static_assert(!std::is_default_constructible_v<OneShotTask>);
static_assert(!std::is_constructible_v<UnscheduledTask, std::atomic<int>*>);
