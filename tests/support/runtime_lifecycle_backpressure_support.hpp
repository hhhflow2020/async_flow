#pragma once

struct TinyThreadTag;

struct TinyRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<TinyThreadTag, 1>());
    static constexpr std::size_t spsc_queue_capacity = 2;
    static constexpr std::size_t external_queue_capacity = 2;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using TinyRuntime = af::AsyncRuntime<TinyRuntimeTraits>;
using TinyTask = TinyRuntime::Task;
using TinyThread = TinyRuntime::Thread;

struct TinyThreads {
    static constexpr TinyThread Logic_0 =
        TinyRuntime::thread_group<TinyThreadTag>().template at<0>();
};

class BlockingTinyTask final : public TinyTask {
public:
    explicit BlockingTinyTask(TinyTask::FactoryToken token) : TinyTask(token) {}

    bool do_it(std::atomic<int> *started, std::atomic<bool> *release, std::atomic<int> *completed) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        return schedule(TinyThreads::Logic_0);
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

    std::atomic<int> *started_{nullptr};
    std::atomic<bool> *release_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class TinyNoopTask final : public TinyTask {
public:
    explicit TinyNoopTask(TinyTask::FactoryToken token) : TinyTask(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *destroyed) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TinyThreads::Logic_0);
    }

    ~TinyNoopTask() override {
        destroyed_->fetch_add(1, std::memory_order_release);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

struct YieldThreadTag;

struct YieldRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<YieldThreadTag, 2>());
    static constexpr std::size_t spsc_queue_capacity = 64;
    static constexpr std::size_t external_queue_capacity = 64;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using YieldRuntime = af::AsyncRuntime<YieldRuntimeTraits>;
using YieldTask = YieldRuntime::Task;
using YieldThread = YieldRuntime::Thread;

struct YieldThreads {
    static constexpr YieldThread Logic_0 =
        YieldRuntime::thread_group<YieldThreadTag>().template at<0>();
    static constexpr YieldThread Logic_1 =
        YieldRuntime::thread_group<YieldThreadTag>().template at<1>();
};

class YieldCountTask final : public YieldTask {
public:
    explicit YieldCountTask(YieldTask::FactoryToken token) : YieldTask(token) {}

    bool do_it(YieldThread thread, std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(thread);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class YieldFanoutTask final : public YieldTask {
public:
    explicit YieldFanoutTask(YieldTask::FactoryToken token) : YieldTask(token) {}

    bool do_it(int child_count, std::atomic<int> *completed, std::atomic<bool> *all_started) {
        child_count_ = child_count;
        completed_ = completed;
        all_started_ = all_started;
        return schedule(YieldThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        for (int i = 0; i < child_count_; ++i) {
            if (!YieldRuntime::start_task<YieldCountTask>(YieldThreads::Logic_0, completed_)) {
                all_started_->store(false, std::memory_order_release);
                return failed();
            }
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int child_count_{0};
    std::atomic<int> *completed_{nullptr};
    std::atomic<bool> *all_started_{nullptr};
};

struct SplitPolicyThreadTag;

struct SplitPolicyRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<SplitPolicyThreadTag, 1>());
    static constexpr std::size_t spsc_queue_capacity = 2;
    static constexpr std::size_t external_queue_capacity = 2;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
};

using SplitPolicyRuntime = af::AsyncRuntime<SplitPolicyRuntimeTraits>;
using SplitPolicyTask = SplitPolicyRuntime::Task;
using SplitPolicyThread = SplitPolicyRuntime::Thread;

struct SplitPolicyThreads {
    static constexpr SplitPolicyThread Logic_0 =
        SplitPolicyRuntime::thread_group<SplitPolicyThreadTag>().template at<0>();
};

class SplitPolicyBlockingTask final : public SplitPolicyTask {
public:
    explicit SplitPolicyBlockingTask(SplitPolicyTask::FactoryToken token)
        : SplitPolicyTask(token) {}

    bool do_it(std::atomic<int> *started, std::atomic<bool> *release, std::atomic<int> *completed) {
        started_ = started;
        release_ = release;
        completed_ = completed;
        return schedule(SplitPolicyThreads::Logic_0);
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

    std::atomic<int> *started_{nullptr};
    std::atomic<bool> *release_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class SplitPolicyCountTask final : public SplitPolicyTask {
public:
    explicit SplitPolicyCountTask(SplitPolicyTask::FactoryToken token) : SplitPolicyTask(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *destroyed = nullptr) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(SplitPolicyThreads::Logic_0);
    }

    ~SplitPolicyCountTask() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

class SplitPolicyFanoutTask final : public SplitPolicyTask {
public:
    explicit SplitPolicyFanoutTask(SplitPolicyTask::FactoryToken token) : SplitPolicyTask(token) {}

    bool do_it(int child_count, std::atomic<int> *completed, std::atomic<bool> *all_started) {
        child_count_ = child_count;
        completed_ = completed;
        all_started_ = all_started;
        return schedule(SplitPolicyThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        for (int i = 0; i < child_count_; ++i) {
            if (!SplitPolicyRuntime::start_task<SplitPolicyCountTask>(completed_)) {
                all_started_->store(false, std::memory_order_release);
                return failed();
            }
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int child_count_{0};
    std::atomic<int> *completed_{nullptr};
    std::atomic<bool> *all_started_{nullptr};
};
