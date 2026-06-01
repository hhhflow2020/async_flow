#if !defined(AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_lifecycle_backpressure_support.hpp is a runtime lifecycle test support detail"
#endif

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
