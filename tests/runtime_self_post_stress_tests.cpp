#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>

#include <gtest/gtest.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_self_post_runtime_config() {
    af::runtime_config config;
    config.threads = {af::cpu_threads("self-post", 1)};
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

class SelfPostChildTask final : public af::runtime_task {
public:
    SelfPostChildTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int id, af::thread_ref target, std::atomic<int> &remaining,
                             std::atomic<int> &failures, std::atomic<int> &sequence,
                             std::atomic<int> *order, int order_capacity,
                             std::atomic<int> &root_completed) noexcept {
        id_ = id;
        target_ = target;
        remaining_ = &remaining;
        failures_ = &failures;
        sequence_ = &sequence;
        order_ = order;
        order_capacity_ = order_capacity;
        root_completed_ = &root_completed;
        return schedule_to(target_);
    }

private:
    af::task_result run_task() noexcept override {
        if (root_completed_->load(std::memory_order_acquire) == 0) {
            failures_->fetch_add(1, std::memory_order_relaxed);
        }

        const int position = sequence_->fetch_add(1, std::memory_order_acq_rel);
        if (position >= 0 && position < order_capacity_) {
            order_[position].store(id_, std::memory_order_release);
        } else {
            failures_->fetch_add(1, std::memory_order_relaxed);
        }
        complete();
        return done();
    }

    void complete() noexcept {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
    }

    int id_{0};
    af::thread_ref target_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *sequence_{nullptr};
    std::atomic<int> *order_{nullptr};
    int order_capacity_{0};
    std::atomic<int> *root_completed_{nullptr};
};

class SelfPostFanoutTask final : public af::runtime_task {
public:
    SelfPostFanoutTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int child_count, af::thread_ref target, std::atomic<int> &remaining,
                             std::atomic<int> &failures, std::atomic<int> &sequence,
                             std::atomic<int> *order, std::atomic<int> &root_completed) noexcept {
        child_count_ = child_count;
        target_ = target;
        remaining_ = &remaining;
        failures_ = &failures;
        sequence_ = &sequence;
        order_ = order;
        root_completed_ = &root_completed;
        return schedule_to(target_);
    }

private:
    af::task_result run_task() noexcept override {
        for (int id = 0; id < child_count_; ++id) {
            remaining_->fetch_add(1, std::memory_order_relaxed);
            auto child = af::make_task<SelfPostChildTask>(owner());
            if (!child->do_it(id, target_, *remaining_, *failures_, *sequence_, order_,
                              child_count_, *root_completed_)) {
                failures_->fetch_add(1, std::memory_order_relaxed);
                if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    af::detail::atomic_notify_one(*remaining_);
                }
            }
            child.reset();
        }

        root_completed_->store(1, std::memory_order_release);
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    int child_count_{0};
    af::thread_ref target_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *failures_{nullptr};
    std::atomic<int> *sequence_{nullptr};
    std::atomic<int> *order_{nullptr};
    std::atomic<int> *root_completed_{nullptr};
};

class SelfAgainTask final : public af::runtime_task {
public:
    SelfAgainTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(int iteration_count, af::thread_ref target,
                             std::atomic<int> &remaining, std::atomic<int> &run_count) noexcept {
        iteration_count_ = iteration_count;
        target_ = target;
        remaining_ = &remaining;
        run_count_ = &run_count;
        return schedule_to(target_);
    }

private:
    af::task_result run_task() noexcept override {
        const int runs = run_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (runs < iteration_count_) {
            return reschedule();
        }
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    int iteration_count_{0};
    af::thread_ref target_{};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *run_count_{nullptr};
};

} // namespace

TEST(RuntimeStressTests, SameThreadFanoutUsesUnifiedInboxAndPreservesFifo) {
    af::runtime runtime(make_self_post_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref threads = runtime.cpu_threads();
    ASSERT_EQ(threads.size(), 1U);

    constexpr int burst_count = 64;
    constexpr int child_count = 256;

    for (int burst = 0; burst < burst_count; ++burst) {
        std::atomic<int> remaining{1};
        std::atomic<int> failures{0};
        std::atomic<int> sequence{0};
        std::atomic<int> root_completed{0};
        std::array<std::atomic<int>, child_count> order{};
        for (auto &value : order) {
            value.store(-1, std::memory_order_relaxed);
        }

        auto task = af::make_task<SelfPostFanoutTask>(runtime);
        if (!task->do_it(child_count, threads.front(), remaining, failures, sequence, order.data(),
                         root_completed)) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "SelfPostFanoutTask::do_it failed at burst " << burst;
            runtime.stop();
            return;
        }
        task.reset();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_zero_until(remaining, deadline)) {
            ADD_FAILURE() << "same-thread fanout did not drain, burst=" << burst
                          << " remaining=" << remaining.load(std::memory_order_acquire)
                          << " failures=" << failures.load(std::memory_order_acquire)
                          << " sequence=" << sequence.load(std::memory_order_acquire)
                          << " root_completed=" << root_completed.load(std::memory_order_acquire);
            runtime.stop();
            return;
        }

        ASSERT_EQ(failures.load(std::memory_order_acquire), 0) << "burst=" << burst;
        ASSERT_EQ(sequence.load(std::memory_order_acquire), child_count) << "burst=" << burst;
        for (int id = 0; id < child_count; ++id) {
            ASSERT_EQ(order[static_cast<std::size_t>(id)].load(std::memory_order_acquire), id)
                << "burst=" << burst << " position=" << id;
        }
    }

    runtime.stop();
}

TEST(RuntimeStressTests, SameThreadAgainUsesUnifiedInboxWithoutCrossThreadHints) {
    af::runtime runtime(make_self_post_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref threads = runtime.cpu_threads();
    ASSERT_EQ(threads.size(), 1U);

    constexpr int task_count = 128;
    constexpr int iteration_count = 64;
    std::atomic<int> remaining{task_count};
    std::array<std::atomic<int>, task_count> runs{};

    for (int i = 0; i < task_count; ++i) {
        auto task = af::make_task<SelfAgainTask>(runtime);
        if (!task->do_it(iteration_count, threads.front(), remaining,
                         runs[static_cast<std::size_t>(i)])) {
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                af::detail::atomic_notify_one(remaining);
            }
            ADD_FAILURE() << "SelfAgainTask::do_it failed at task " << i;
            runtime.stop();
            return;
        }
        task.reset();
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (!wait_zero_until(remaining, deadline)) {
        int min_runs = iteration_count;
        int min_id = -1;
        for (int i = 0; i < task_count; ++i) {
            const int value = runs[static_cast<std::size_t>(i)].load(std::memory_order_acquire);
            if (value < min_runs) {
                min_runs = value;
                min_id = i;
            }
        }
        ADD_FAILURE() << "same-thread again tasks did not drain, remaining="
                      << remaining.load(std::memory_order_acquire) << " min_id=" << min_id
                      << " min_runs=" << min_runs;
        runtime.stop();
        return;
    }

    for (int i = 0; i < task_count; ++i) {
        EXPECT_EQ(runs[static_cast<std::size_t>(i)].load(std::memory_order_acquire),
                  iteration_count)
            << "task=" << i;
    }
    runtime.stop();
}
