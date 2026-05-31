#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/async_flow.hpp"

namespace af::test::runtime_scheduler_stress {

enum class RepeatHopThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    enum_thread_index_end,
};

struct RepeatHopRuntimeTraits {
    using Thread = RepeatHopThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(RepeatHopThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using RepeatHopRuntime = af::AsyncRuntime<RepeatHopRuntimeTraits>;
using RepeatHopTaskBase = RepeatHopRuntime::Task;

class RepeatHopTask final : public RepeatHopTaskBase {
public:
    explicit RepeatHopTask(RepeatHopTaskBase::FactoryToken token) : RepeatHopTaskBase(token) {}

    bool do_it(
        int hops,
        int id,
        std::atomic<int>* remaining,
        std::atomic<int>* runs,
        std::atomic<int>* post_failures,
        std::atomic<int>* progress,
        std::atomic<int>* last_thread) {
        hops_ = hops;
        id_ = id;
        remaining_ = remaining;
        runs_ = runs;
        post_failures_ = post_failures;
        progress_ = progress;
        last_thread_ = last_thread;
        return schedule(RepeatHopThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        progress_[id_].fetch_add(1, std::memory_order_relaxed);
        last_thread_[id_].store(
            RepeatHopRuntime::current_thread() == RepeatHopThread::Logic_0 ? 0 : 1,
            std::memory_order_relaxed);
        if (hops_-- > 0) {
            const auto next = RepeatHopRuntime::current_thread() == RepeatHopThread::Logic_0
                ? RepeatHopThread::Logic_1
                : RepeatHopThread::Logic_0;
            if (!schedule(next)) {
                post_failures_->fetch_add(1, std::memory_order_relaxed);
                if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    remaining_->notify_one();
                }
                return failed();
            }
            return pending();
        }

        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    int hops_{0};
    int id_{0};
    std::atomic<int>* remaining_{nullptr};
    std::atomic<int>* runs_{nullptr};
    std::atomic<int>* post_failures_{nullptr};
    std::atomic<int>* progress_{nullptr};
    std::atomic<int>* last_thread_{nullptr};
};

enum class WideHopThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_64 = 64,
    enum_thread_index_end,
};

struct WideHopRuntimeTraits {
    using Thread = WideHopThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(WideHopThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 256;
    static constexpr std::size_t external_queue_capacity = 256;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using WideHopRuntime = af::AsyncRuntime<WideHopRuntimeTraits>;
using WideHopTaskBase = WideHopRuntime::Task;

class WideHopTask final : public WideHopTaskBase {
public:
    explicit WideHopTask(WideHopTaskBase::FactoryToken token) : WideHopTaskBase(token) {}

    bool do_it(
        int hops,
        std::atomic<int>* remaining,
        std::atomic<int>* runs,
        std::atomic<int>* post_failures) {
        hops_ = hops;
        remaining_ = remaining;
        runs_ = runs;
        post_failures_ = post_failures;
        return schedule(WideHopThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        if (hops_-- > 0) {
            const auto next = WideHopRuntime::current_thread_index() == 0
                ? WideHopThread::Logic_64
                : WideHopThread::Logic_0;
            if (!schedule(next)) {
                post_failures_->fetch_add(1, std::memory_order_relaxed);
                if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    remaining_->notify_one();
                }
                return failed();
            }
            return pending();
        }

        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    int hops_{0};
    std::atomic<int>* remaining_{nullptr};
    std::atomic<int>* runs_{nullptr};
    std::atomic<int>* post_failures_{nullptr};
};

enum class ParallelResumeThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    enum_thread_index_end,
};

struct ParallelResumeRuntimeTraits {
    using Thread = ParallelResumeThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(ParallelResumeThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using ParallelResumeRuntime = af::AsyncRuntime<ParallelResumeRuntimeTraits>;
using ParallelResumeTaskBase = ParallelResumeRuntime::Task;

class ParallelResumeTask final : public ParallelResumeTaskBase {
public:
    explicit ParallelResumeTask(ParallelResumeTaskBase::FactoryToken token)
        : ParallelResumeTaskBase(token) {}

    bool do_it(
        int id,
        std::atomic<int>* remaining,
        std::atomic<int>* completed,
        std::atomic<int>* failures,
        std::atomic<int>* shard_runs,
        std::atomic<std::uint64_t>* sum,
        std::atomic<int>* task_stage,
        std::atomic<int>* task_shards) {
        id_ = id;
        remaining_ = remaining;
        completed_ = completed;
        failures_ = failures;
        shard_runs_ = shard_runs;
        sum_ = sum;
        task_stage_ = task_stage;
        task_shards_ = task_shards;
        task_stage_[id_].store(1, std::memory_order_relaxed);
        ops_ = af::ShardedOps<std::uint64_t>(4);
        for (std::uint64_t value = 0; value < 1024; ++value) {
            ops_.shards[value & 3U].push_back(value);
        }
        return schedule(ParallelResumeThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Split,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            task_stage_[id_].store(2, std::memory_order_relaxed);
            ParallelResumeRuntime::parallel_shards(
                ParallelResumeThread::Logic_0,
                ops_,
                af::ParallelMode::AllShards,
                this,
                [this](std::uint16_t shard, std::vector<std::uint64_t>& shard_ops) {
                    task_shards_[id_ * 4 + shard].fetch_add(1, std::memory_order_relaxed);
                    shard_runs_->fetch_add(1, std::memory_order_relaxed);
                    std::uint64_t local = 0;
                    for (const std::uint64_t value : shard_ops) {
                        local += value;
                    }
                    sum_->fetch_add(local, std::memory_order_relaxed);
                });
            task_stage_[id_].store(3, std::memory_order_relaxed);
            return pending();

        case State::Finish:
            task_stage_[id_].store(4, std::memory_order_relaxed);
            if (last_parallel_failures() != 0U) {
                failures_->fetch_add(1, std::memory_order_relaxed);
            }
            completed_->fetch_add(1, std::memory_order_relaxed);
            if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                remaining_->notify_one();
            }
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    af::ShardedOps<std::uint64_t> ops_{4};
    int id_{0};
    std::atomic<int>* remaining_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* failures_{nullptr};
    std::atomic<int>* shard_runs_{nullptr};
    std::atomic<std::uint64_t>* sum_{nullptr};
    std::atomic<int>* task_stage_{nullptr};
    std::atomic<int>* task_shards_{nullptr};
};

inline bool wait_zero_until(
    std::atomic<int>& remaining,
    std::chrono::steady_clock::time_point deadline) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

} // namespace af::test::runtime_scheduler_stress
