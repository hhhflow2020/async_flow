#if !defined(AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_DETAIL_INCLUDE)
#error "runtime_scheduler_stress_running_wake_terminal.hpp is a runtime scheduler stress support detail"
#endif

enum class RunningWakeTerminalMode : std::uint8_t {
    Done,
    Again,
};

class RunningWakeTerminalOwnerTask;

class RunningWakeTerminalWakerTask final : public RunningPendingTaskBase {
public:
    explicit RunningWakeTerminalWakerTask(RunningPendingTaskBase::FactoryToken token)
        : RunningPendingTaskBase(token) {}

    bool do_it(
        RunningWakeTerminalOwnerTask* owner,
        std::atomic<int>* wake_flag,
        std::atomic<int>* wake_attempts,
        std::atomic<int>* failures);

private:
    af::TaskResult run() override;

    RunningWakeTerminalOwnerTask* owner_{nullptr};
    std::atomic<int>* wake_flag_{nullptr};
    std::atomic<int>* wake_attempts_{nullptr};
    std::atomic<int>* failures_{nullptr};
};

class RunningWakeTerminalOwnerTask final : public RunningPendingTaskBase {
public:
    explicit RunningWakeTerminalOwnerTask(RunningPendingTaskBase::FactoryToken token)
        : RunningPendingTaskBase(token) {}

    bool do_it(
        RunningWakeTerminalMode mode,
        int id,
        std::atomic<int>* remaining,
        std::atomic<int>* completed,
        std::atomic<int>* wake_attempts,
        std::atomic<int>* failures,
        std::atomic<int>* wake_flags) {
        mode_ = mode;
        id_ = id;
        remaining_ = remaining;
        completed_ = completed;
        wake_attempts_ = wake_attempts;
        failures_ = failures;
        wake_flag_ = &wake_flags[id_];
        return schedule(RunningPendingThread::Owner);
    }

    bool request_resume_from_waker() noexcept {
        return schedule(RunningPendingThread::Owner);
    }

private:
    enum class State : std::uint8_t {
        ArmWake,
        FinishAgain,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ArmWake:
            if (!RunningPendingRuntime::start_task<RunningWakeTerminalWakerTask>(
                    this,
                    wake_flag_,
                    wake_attempts_,
                    failures_)) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                complete();
                return failed();
            }

            if (!wait_for_wake()) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                complete();
                return failed();
            }

            if (mode_ == RunningWakeTerminalMode::Done) {
                completed_->fetch_add(1, std::memory_order_relaxed);
                complete();
                return done();
            }

            state_ = State::FinishAgain;
            return again();

        case State::FinishAgain:
            completed_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return done();
        }

        failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
        return failed();
    }

    [[nodiscard]] bool wait_for_wake() const noexcept {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (wake_flag_->load(std::memory_order_acquire) == 0 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return wake_flag_->load(std::memory_order_acquire) != 0;
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
    }

    RunningWakeTerminalMode mode_{RunningWakeTerminalMode::Done};
    State state_{State::ArmWake};
    int id_{0};
    std::atomic<int>* remaining_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* wake_attempts_{nullptr};
    std::atomic<int>* failures_{nullptr};
    std::atomic<int>* wake_flag_{nullptr};
};

inline bool RunningWakeTerminalWakerTask::do_it(
    RunningWakeTerminalOwnerTask* owner,
    std::atomic<int>* wake_flag,
    std::atomic<int>* wake_attempts,
    std::atomic<int>* failures) {
    owner_ = owner;
    wake_flag_ = wake_flag;
    wake_attempts_ = wake_attempts;
    failures_ = failures;
    return schedule(RunningPendingThread::Waker);
}

inline af::TaskResult RunningWakeTerminalWakerTask::run() {
    if (!owner_->request_resume_from_waker()) {
        failures_->fetch_add(1, std::memory_order_relaxed);
    }
    wake_flag_->store(1, std::memory_order_release);
    wake_attempts_->fetch_add(1, std::memory_order_release);
    wake_attempts_->notify_one();
    return done();
}
