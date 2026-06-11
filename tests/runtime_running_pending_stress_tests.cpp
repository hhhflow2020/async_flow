#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"

namespace {

enum class RunningWakeTerminalMode : std::uint8_t {
    Done,
    Again,
};

[[nodiscard]] af::runtime_config make_running_pending_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("running-owner", 1),
        af::cpu_threads("running-waker", 1),
    };
    config.logger.consumer_thread = af::thread_selector::any_cpu();
    config.diagnostics.enable_thread_name = false;
    return config;
}

[[nodiscard]] bool wait_zero_until(std::atomic<int> &value,
                                   std::chrono::steady_clock::time_point deadline) {
    while (value.load(std::memory_order_acquire) != 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return value.load(std::memory_order_acquire) == 0;
        }

        const int observed = value.load(std::memory_order_acquire);
        if (observed == 0) {
            return true;
        }
        const auto remaining = deadline - now;
        const auto wait_for =
            remaining < std::chrono::milliseconds(1) ? remaining : std::chrono::milliseconds(1);
        static_cast<void>(af::detail::atomic_wait_value_for(value, observed, wait_for,
                                                            std::memory_order_acquire));
    }
    return true;
}

[[nodiscard]] bool wait_non_zero_until(std::atomic<int> &value,
                                       std::chrono::steady_clock::time_point deadline) {
    while (value.load(std::memory_order_acquire) == 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return value.load(std::memory_order_acquire) != 0;
        }

        const auto remaining = deadline - now;
        const auto wait_for =
            remaining < std::chrono::milliseconds(1) ? remaining : std::chrono::milliseconds(1);
        static_cast<void>(
            af::detail::atomic_wait_value_for(value, 0, wait_for, std::memory_order_acquire));
    }
    return true;
}

class RunningPendingOwnerTask;

class RunningPendingWakerTask final : public af::runtime_task {
public:
    RunningPendingWakerTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(RunningPendingOwnerTask &owner_task, af::thread_ref waker_thread,
                             std::atomic<int> &wake_flag, std::atomic<int> &wake_attempts,
                             std::atomic<int> &failures) noexcept {
        owner_task_ = &owner_task;
        waker_thread_ = waker_thread;
        wake_flag_ = &wake_flag;
        wake_attempts_ = &wake_attempts;
        failures_ = &failures;
        return schedule_to(waker_thread_);
    }

private:
    af::task_result run_task() noexcept override;

    RunningPendingOwnerTask *owner_task_{nullptr};
    af::thread_ref waker_thread_{};
    std::atomic<int> *wake_flag_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
};

class RunningPendingOwnerTask final : public af::runtime_task {
public:
    RunningPendingOwnerTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int id, af::thread_ref owner_thread, af::thread_ref waker_thread,
                             std::atomic<int> &remaining, std::atomic<int> &completed,
                             std::atomic<int> &wake_attempts, std::atomic<int> &failures,
                             std::atomic<int> *stages, std::atomic<int> *wake_flags) noexcept {
        id_ = id;
        owner_thread_ = owner_thread;
        waker_thread_ = waker_thread;
        remaining_ = &remaining;
        completed_ = &completed;
        wake_attempts_ = &wake_attempts;
        failures_ = &failures;
        stages_ = stages;
        wake_flag_ = &wake_flags[id_];
        stages_[id_].store(1, std::memory_order_relaxed);
        return schedule_to(owner_thread_);
    }

    [[nodiscard]] bool request_resume_from_waker() noexcept {
        return schedule_to(owner_thread_);
    }

private:
    enum class State : std::uint8_t {
        ArmWake,
        Finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case State::ArmWake:
            return arm_waker();
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

    af::task_result arm_waker() noexcept {
        state_ = State::Finish;
        stages_[id_].store(2, std::memory_order_release);
        auto waker = af::make_task<RunningPendingWakerTask>(owner());
        if (!waker->do_it(*this, waker_thread_, *wake_flag_, *wake_attempts_, *failures_)) {
            failures_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return failed();
        }
        waker.reset();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_non_zero_until(*wake_flag_, deadline)) {
            failures_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return failed();
        }
        stages_[id_].store(3, std::memory_order_release);
        return pending();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    State state_{State::ArmWake};
    int id_{0};
    af::thread_ref owner_thread_{};
    af::thread_ref waker_thread_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *stages_{nullptr};
    std::atomic<int> *wake_flag_{nullptr};
};

af::task_result RunningPendingWakerTask::run_task() noexcept {
    if (owner_task_ == nullptr || !owner_task_->request_resume_from_waker()) {
        failures_->fetch_add(1, std::memory_order_relaxed);
    }
    wake_flag_->store(1, std::memory_order_release);
    af::detail::atomic_notify_one(*wake_flag_);
    wake_attempts_->fetch_add(1, std::memory_order_release);
    af::detail::atomic_notify_one(*wake_attempts_);
    return done();
}

class RunningWakeTerminalOwnerTask;

class RunningWakeTerminalWakerTask final : public af::runtime_task {
public:
    RunningWakeTerminalWakerTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(RunningWakeTerminalOwnerTask &owner_task, af::thread_ref waker_thread,
                             std::atomic<int> &wake_flag, std::atomic<int> &wake_attempts,
                             std::atomic<int> &failures) noexcept {
        owner_task_ = &owner_task;
        waker_thread_ = waker_thread;
        wake_flag_ = &wake_flag;
        wake_attempts_ = &wake_attempts;
        failures_ = &failures;
        return schedule_to(waker_thread_);
    }

private:
    af::task_result run_task() noexcept override;

    RunningWakeTerminalOwnerTask *owner_task_{nullptr};
    af::thread_ref waker_thread_{};
    std::atomic<int> *wake_flag_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
};

class RunningWakeTerminalOwnerTask final : public af::runtime_task {
public:
    RunningWakeTerminalOwnerTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(RunningWakeTerminalMode mode, int id, af::thread_ref owner_thread,
                             af::thread_ref waker_thread, std::atomic<int> &remaining,
                             std::atomic<int> &completed, std::atomic<int> &wake_attempts,
                             std::atomic<int> &failures, std::atomic<int> *wake_flags) noexcept {
        mode_ = mode;
        id_ = id;
        owner_thread_ = owner_thread;
        waker_thread_ = waker_thread;
        remaining_ = &remaining;
        completed_ = &completed;
        wake_attempts_ = &wake_attempts;
        failures_ = &failures;
        wake_flag_ = &wake_flags[id_];
        return schedule_to(owner_thread_);
    }

    [[nodiscard]] bool request_resume_from_waker() noexcept {
        return schedule_to(owner_thread_);
    }

private:
    enum class State : std::uint8_t {
        ArmWake,
        FinishAgain,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case State::ArmWake:
            return arm_terminal_waker();
        case State::FinishAgain:
            completed_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return done();
        }

        failures_->fetch_add(1, std::memory_order_relaxed);
        complete();
        return failed();
    }

    af::task_result arm_terminal_waker() noexcept {
        auto waker = af::make_task<RunningWakeTerminalWakerTask>(owner());
        if (!waker->do_it(*this, waker_thread_, *wake_flag_, *wake_attempts_, *failures_)) {
            failures_->fetch_add(1, std::memory_order_relaxed);
            complete();
            return failed();
        }
        waker.reset();

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
        return reschedule();
    }

    [[nodiscard]] bool wait_for_wake() const noexcept {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        return wait_non_zero_until(*wake_flag_, deadline);
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    RunningWakeTerminalMode mode_{RunningWakeTerminalMode::Done};
    State state_{State::ArmWake};
    int id_{0};
    af::thread_ref owner_thread_{};
    af::thread_ref waker_thread_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *wake_attempts_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *wake_flag_{nullptr};
};

af::task_result RunningWakeTerminalWakerTask::run_task() noexcept {
    if (owner_task_ == nullptr || !owner_task_->request_resume_from_waker()) {
        failures_->fetch_add(1, std::memory_order_relaxed);
    }
    wake_flag_->store(1, std::memory_order_release);
    af::detail::atomic_notify_one(*wake_flag_);
    wake_attempts_->fetch_add(1, std::memory_order_release);
    af::detail::atomic_notify_one(*wake_attempts_);
    return done();
}

void run_terminal_wake_case(RunningWakeTerminalMode mode) {
    af::runtime runtime(make_running_pending_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_ref owner_thread = runtime.thread_group("running-owner").front();
    const af::thread_ref waker_thread = runtime.thread_group("running-waker").front();
    ASSERT_TRUE(owner_thread);
    ASSERT_TRUE(waker_thread);

    constexpr int burst_count = 32;
    constexpr int tasks_per_burst = 32;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> completed{0};
        std::atomic<int> wake_attempts{0};
        std::atomic<int> failures{0};
        std::array<std::atomic<int>, tasks_per_burst> wake_flags{};

        for (int i = 0; i < tasks_per_burst; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<RunningWakeTerminalOwnerTask>(runtime);
            if (!task->do_it(mode, i, owner_thread, waker_thread, remaining, completed,
                             wake_attempts, failures, wake_flags.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RunningWakeTerminalOwnerTask::do_it failed at burst " << burst
                              << " task " << i;
                runtime.stop();
                return;
            }
            task.reset();
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            ADD_FAILURE() << "terminal running wake did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " completed=" << completed.load(std::memory_order_acquire)
                          << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire);
            runtime.stop();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0)
            << "burst=" << burst << " completed=" << completed.load(std::memory_order_acquire)
            << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire);
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(wake_attempts.load(std::memory_order_acquire), tasks_per_burst)
            << "burst=" << burst;
    }

    runtime.stop();
}

} // namespace

TEST(RuntimeStressTests, RunningToPendingWakeDoesNotStrandOwner) {
    af::runtime runtime(make_running_pending_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_ref owner_thread = runtime.thread_group("running-owner").front();
    const af::thread_ref waker_thread = runtime.thread_group("running-waker").front();
    ASSERT_TRUE(owner_thread);
    ASSERT_TRUE(waker_thread);

    constexpr int burst_count = 128;
    constexpr int tasks_per_burst = 64;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{0};
        std::atomic<int> completed{0};
        std::atomic<int> wake_attempts{0};
        std::atomic<int> failures{0};
        std::array<std::atomic<int>, tasks_per_burst> stages{};
        std::array<std::atomic<int>, tasks_per_burst> wake_flags{};

        for (int i = 0; i < tasks_per_burst; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<RunningPendingOwnerTask>(runtime);
            if (!task->do_it(i, owner_thread, waker_thread, remaining, completed, wake_attempts,
                             failures, stages.data(), wake_flags.data())) {
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(remaining);
                }
                ADD_FAILURE() << "RunningPendingOwnerTask::do_it failed at burst " << burst
                              << " task " << i;
                runtime.stop();
                return;
            }
            task.reset();
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            int min_stage = 4;
            int min_id = -1;
            for (int i = 0; i < tasks_per_burst; ++i) {
                const int stage =
                    stages[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
                if (stage < min_stage) {
                    min_stage = stage;
                    min_id = i;
                }
            }
            ADD_FAILURE() << "running-to-pending wake did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " completed=" << completed.load(std::memory_order_acquire)
                          << " wake_attempts=" << wake_attempts.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire)
                          << " min_id=" << min_id << " min_stage=" << min_stage;
            runtime.stop();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(completed.load(std::memory_order_acquire), tasks_per_burst) << "burst=" << burst;
        ASSERT_EQ(wake_attempts.load(std::memory_order_acquire), tasks_per_burst)
            << "burst=" << burst;
    }

    runtime.stop();
}

TEST(RuntimeStressTests, RunningWakeBeforeDoneIsBenign) {
    run_terminal_wake_case(RunningWakeTerminalMode::Done);
}

TEST(RuntimeStressTests, RunningWakeBeforeAgainIsBenign) {
    run_terminal_wake_case(RunningWakeTerminalMode::Again);
}
