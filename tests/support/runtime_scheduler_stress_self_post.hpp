#pragma once

struct SelfPostThreadTag;

struct SelfPostRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<SelfPostThreadTag, 1>());
    static constexpr std::size_t spsc_queue_capacity = 4096;
    static constexpr std::size_t external_queue_capacity = 4096;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using SelfPostRuntime = af::AsyncRuntime<SelfPostRuntimeTraits>;
using SelfPostTaskBase = SelfPostRuntime::Task;
using SelfPostThread = SelfPostRuntime::Thread;

struct SelfPostThreads {
    static constexpr SelfPostThread Logic =
        SelfPostRuntime::thread_group<SelfPostThreadTag>().template at<0>();
};

class SelfPostChildTask final : public SelfPostTaskBase {
public:
    explicit SelfPostChildTask(SelfPostTaskBase::FactoryToken token) : SelfPostTaskBase(token) {}

    bool do_it(int id, std::atomic<int> *remaining, std::atomic<int> *failures,
               std::atomic<int> *sequence, std::atomic<int> *order, int order_capacity,
               std::atomic<int> *root_completed) {
        id_ = id;
        remaining_ = remaining;
        failures_ = failures;
        sequence_ = sequence;
        order_ = order;
        order_capacity_ = order_capacity;
        root_completed_ = root_completed;
        return schedule(SelfPostThreads::Logic);
    }

private:
    af::TaskResult run() override {
        if (root_completed_->load(std::memory_order_acquire) == 0) {
            failures_->fetch_add(1, std::memory_order_relaxed);
        }
        const int position = sequence_->fetch_add(1, std::memory_order_acq_rel);
        if (position >= 0 && position < order_capacity_) {
            order_[position].store(id_, std::memory_order_release);
        } else {
            failures_->fetch_add(1, std::memory_order_relaxed);
        }
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    int id_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *sequence_{nullptr};
    std::atomic<int> *order_{nullptr};
    int order_capacity_{0};
    std::atomic<int> *root_completed_{nullptr};
};

class SelfPostFanoutTask final : public SelfPostTaskBase {
public:
    explicit SelfPostFanoutTask(SelfPostTaskBase::FactoryToken token) : SelfPostTaskBase(token) {}

    bool do_it(int child_count, std::atomic<int> *remaining, std::atomic<int> *failures,
               std::atomic<int> *sequence, std::atomic<int> *order,
               std::atomic<int> *root_completed) {
        child_count_ = child_count;
        remaining_ = remaining;
        failures_ = failures;
        sequence_ = sequence;
        order_ = order;
        root_completed_ = root_completed;
        return schedule(SelfPostThreads::Logic);
    }

private:
    af::TaskResult run() override {
        for (int id = 0; id < child_count_; ++id) {
            remaining_->fetch_add(1, std::memory_order_relaxed);
            if (!SelfPostRuntime::start_task<SelfPostChildTask>(
                    id, remaining_, failures_, sequence_, order_, child_count_, root_completed_)) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(*remaining_);
                }
            }
        }

        root_completed_->store(1, std::memory_order_release);
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    int child_count_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *sequence_{nullptr};
    std::atomic<int> *order_{nullptr};
    std::atomic<int> *root_completed_{nullptr};
};

class SelfAgainTask final : public SelfPostTaskBase {
public:
    explicit SelfAgainTask(SelfPostTaskBase::FactoryToken token) : SelfPostTaskBase(token) {}

    bool do_it(int iteration_count, std::atomic<int> *remaining, std::atomic<int> *run_count) {
        iteration_count_ = iteration_count;
        remaining_ = remaining;
        run_count_ = run_count;
        return schedule(SelfPostThreads::Logic);
    }

private:
    af::TaskResult run() override {
        const int runs = run_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (runs < iteration_count_) {
            return again();
        }
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    int iteration_count_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *run_count_{nullptr};
};
