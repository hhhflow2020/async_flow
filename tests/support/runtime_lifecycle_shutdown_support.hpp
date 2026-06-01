#if !defined(AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_lifecycle_shutdown_support.hpp is a runtime lifecycle test support detail"
#endif

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
    static constexpr bool enable_task_registry = true;
};

using FastShutdownRuntime = af::AsyncRuntime<FastShutdownRuntimeTraits>;
using FastShutdownTaskBase = FastShutdownRuntime::Task;

class FastShutdownPendingTask final : public FastShutdownTaskBase {
public:
    explicit FastShutdownPendingTask(FastShutdownTaskBase::FactoryToken token)
        : FastShutdownTaskBase(token) {}

    bool do_it(std::atomic<int>* entered, std::atomic<int>* destroyed = nullptr) {
        entered_ = entered;
        destroyed_ = destroyed;
        return schedule(FastShutdownThread::Logic_0);
    }

    ~FastShutdownPendingTask() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::TaskResult run() override {
        entered_->fetch_add(1, std::memory_order_release);
        entered_->notify_one();
        return pending();
    }

    std::atomic<int>* entered_{nullptr};
    std::atomic<int>* destroyed_{nullptr};
};

static_assert(!std::is_default_constructible_v<NoInitTask>);
