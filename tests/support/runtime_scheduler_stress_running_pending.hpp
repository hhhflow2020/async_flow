#pragma once

struct RunningPendingOwnerThreadTag;
struct RunningPendingWakerThreadTag;

struct RunningPendingRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<RunningPendingOwnerThreadTag, 1>(),
                          af::thread_group<RunningPendingWakerThreadTag, 1>());
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using RunningPendingRuntime = af::AsyncRuntime<RunningPendingRuntimeTraits>;
using RunningPendingTaskBase = RunningPendingRuntime::Task;
using RunningPendingThread = RunningPendingRuntime::Thread;

struct RunningPendingThreads {
    static constexpr RunningPendingThread Owner =
        RunningPendingRuntime::thread_group<RunningPendingOwnerThreadTag>().template at<0>();
    static constexpr RunningPendingThread Waker =
        RunningPendingRuntime::thread_group<RunningPendingWakerThreadTag>().template at<0>();
};

class RunningPendingOwnerTask;

class RunningPendingWakerTask final : public RunningPendingTaskBase {
public:
    explicit RunningPendingWakerTask(RunningPendingTaskBase::FactoryToken token)
        : RunningPendingTaskBase(token) {}

    bool do_it(RunningPendingOwnerTask *owner, std::atomic<int> *wake_flag,
               std::atomic<int> *wake_attempts, std::atomic<int> *failures);

private:
    af::TaskResult run() override;

    RunningPendingOwnerTask *owner_{nullptr};
    std::atomic<int> *wake_flag_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
};

class RunningPendingOwnerTask final : public RunningPendingTaskBase {
public:
    explicit RunningPendingOwnerTask(RunningPendingTaskBase::FactoryToken token)
        : RunningPendingTaskBase(token) {}

    bool do_it(int id, std::atomic<int> *remaining, std::atomic<int> *completed,
               std::atomic<int> *wake_attempts, std::atomic<int> *failures,
               std::atomic<int> *stages, std::atomic<int> *wake_flags) {
        id_ = id;
        remaining_ = remaining;
        completed_ = completed;
        wake_attempts_ = wake_attempts;
        failures_ = failures;
        stages_ = stages;
        wake_flag_ = &wake_flags[id_];
        stages_[id_].store(1, std::memory_order_relaxed);
        return schedule(RunningPendingThreads::Owner);
    }

    bool request_resume_from_waker() noexcept {
        return schedule(RunningPendingThreads::Owner);
    }

private:
    enum class State : std::uint8_t {
        ArmWake,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ArmWake: {
            state_ = State::Finish;
            stages_[id_].store(2, std::memory_order_release);
            if (!RunningPendingRuntime::start_task<RunningPendingWakerTask>(
                    this, wake_flag_, wake_attempts_, failures_)) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                complete();
                return failed();
            }

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (wake_flag_->load(std::memory_order_acquire) == 0 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            if (wake_flag_->load(std::memory_order_acquire) == 0) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                complete();
                return failed();
            }
            stages_[id_].store(3, std::memory_order_release);
            return pending();
        }

        case State::Finish:
            stages_[id_].store(4, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return done();
        }

        failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
        return failed();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    State state_{State::ArmWake};
    int id_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *stages_{nullptr};
    std::atomic<int> *wake_flag_{nullptr};
};

inline bool RunningPendingWakerTask::do_it(RunningPendingOwnerTask *owner,
                                           std::atomic<int> *wake_flag,
                                           std::atomic<int> *wake_attempts,
                                           std::atomic<int> *failures) {
    owner_ = owner;
    wake_flag_ = wake_flag;
    wake_attempts_ = wake_attempts;
    failures_ = failures;
    return schedule(RunningPendingThreads::Waker);
}

inline af::TaskResult RunningPendingWakerTask::run() {
    if (!owner_->request_resume_from_waker()) {
        failures_->fetch_add(1, std::memory_order_relaxed);
    }
    wake_flag_->store(1, std::memory_order_release);
    wake_attempts_->fetch_add(1, std::memory_order_release);
    af::detail::atomic_notify_one(*wake_attempts_);
    return done();
}
