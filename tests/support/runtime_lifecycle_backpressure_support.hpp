#pragma once

struct TinyThreadTag;

struct TinyRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<TinyThreadTag, 1>());
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
        af::detail::atomic_notify_one(*started_);
        while (!release_->load(std::memory_order_acquire)) {
            af::detail::atomic_wait_value(*release_, false, std::memory_order_acquire);
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

    bool do_it(std::atomic<int> *completed, std::atomic<int> *destroyed,
               af::ScheduleMode mode = af::ScheduleMode::Auto) {
        completed_ = completed;
        destroyed_ = destroyed;
        return schedule(TinyThreads::Logic_0, mode);
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

class TinySelfOrderedRouteTask final : public TinyTask {
public:
    explicit TinySelfOrderedRouteTask(TinyTask::FactoryToken token) : TinyTask(token) {}

    bool do_it(std::atomic<int> *child_completed, std::atomic<int> *parent_completed,
               std::atomic<int> *auto_accepted, std::atomic<int> *ordered_accepted,
               std::atomic<int> *destroyed) {
        child_completed_ = child_completed;
        parent_completed_ = parent_completed;
        auto_accepted_ = auto_accepted;
        ordered_accepted_ = ordered_accepted;
        destroyed_ = destroyed;
        return schedule(TinyThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        static_cast<void>(TinyRuntime::start_task<TinyNoopTask>(child_completed_, destroyed_));
        static_cast<void>(TinyRuntime::start_task<TinyNoopTask>(child_completed_, destroyed_));

        if (TinyRuntime::start_task<TinyNoopTask>(child_completed_, destroyed_)) {
            auto_accepted_->fetch_add(1, std::memory_order_release);
        }
        if (TinyRuntime::start_task<TinyNoopTask>(child_completed_, destroyed_,
                                                  af::ScheduleMode::Ordered)) {
            ordered_accepted_->fetch_add(1, std::memory_order_release);
        }

        parent_completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *child_completed_{nullptr};
    std::atomic<int> *parent_completed_{nullptr};
    std::atomic<int> *auto_accepted_{nullptr};
    std::atomic<int> *ordered_accepted_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

struct TwoThreadTag;

struct TwoThreadRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<TwoThreadTag, 2>());
};

using TwoThreadRuntime = af::AsyncRuntime<TwoThreadRuntimeTraits>;
using TwoThreadTask = TwoThreadRuntime::Task;
using TwoThread = TwoThreadRuntime::Thread;

struct TwoThreads {
    static constexpr TwoThread Logic_0 =
        TwoThreadRuntime::thread_group<TwoThreadTag>().template at<0>();
    static constexpr TwoThread Logic_1 =
        TwoThreadRuntime::thread_group<TwoThreadTag>().template at<1>();
};

class TwoThreadCountTask final : public TwoThreadTask {
public:
    explicit TwoThreadCountTask(TwoThreadTask::FactoryToken token) : TwoThreadTask(token) {}

    bool do_it(TwoThread thread, std::atomic<int> *completed) {
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

class TwoThreadFanoutTask final : public TwoThreadTask {
public:
    explicit TwoThreadFanoutTask(TwoThreadTask::FactoryToken token) : TwoThreadTask(token) {}

    bool do_it(int child_count, std::atomic<int> *completed, std::atomic<bool> *all_started) {
        child_count_ = child_count;
        completed_ = completed;
        all_started_ = all_started;
        return schedule(TwoThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        for (int i = 0; i < child_count_; ++i) {
            if (!TwoThreadRuntime::start_task<TwoThreadCountTask>(TwoThreads::Logic_0,
                                                                  completed_)) {
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
