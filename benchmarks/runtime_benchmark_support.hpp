#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/async_flow.hpp"

namespace af_bench::runtime {

struct BenchLogicThreadTag;
struct BenchIoThreadTag;

struct BenchRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<BenchLogicThreadTag, 4, af::ThreadKind::Worker, "bench-log">(),
        af::thread_group<BenchIoThreadTag, 1, af::ThreadKind::Epoll, "bench-io">());
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using Runtime = af::AsyncRuntime<BenchRuntimeTraits>;
using Task = Runtime::Task;
using BenchThread = Runtime::Thread;

struct BenchThreads {
    static constexpr BenchThread Logic_0 =
        Runtime::thread_group<BenchLogicThreadTag>().template at<0>();
    static constexpr BenchThread Logic_1 =
        Runtime::thread_group<BenchLogicThreadTag>().template at<1>();
    static constexpr BenchThread Logic_2 =
        Runtime::thread_group<BenchLogicThreadTag>().template at<2>();
    static constexpr BenchThread Logic_3 =
        Runtime::thread_group<BenchLogicThreadTag>().template at<3>();
    static constexpr BenchThread IO_0 = Runtime::thread_group<BenchIoThreadTag>().template at<0>();
};

inline void wait_zero(std::atomic<int> &remaining) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        const int observed = remaining.load(std::memory_order_acquire);
        if (observed != 0) {
            remaining.wait(observed, std::memory_order_acquire);
        }
    }
}

inline void undo_remaining(std::atomic<int> &remaining) {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        remaining.notify_one();
    }
}

class CountTask final : public Task {
public:
    explicit CountTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(BenchThread thread, std::atomic<int> *remaining) {
        remaining_ = remaining;
        return schedule(thread);
    }

private:
    af::TaskResult run() override {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    std::atomic<int> *remaining_{nullptr};
};

class HopTask final : public Task {
public:
    explicit HopTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int hops, std::atomic<int> *remaining) {
        hops_ = hops;
        remaining_ = remaining;
        return schedule(BenchThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        if (hops_-- > 0) {
            const auto next = Runtime::current_thread() == BenchThreads::Logic_0
                                  ? BenchThreads::Logic_1
                                  : BenchThreads::Logic_0;
            return pending_on(next);
        }

        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    int hops_{0};
    std::atomic<int> *remaining_{nullptr};
};

class IoHopTask final : public Task {
public:
    explicit IoHopTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *remaining) {
        remaining_ = remaining;
        state_ = State::Logic;
        return schedule(BenchThreads::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Logic,
        Io,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Logic:
            state_ = State::Io;
            return pending_on(BenchThreads::IO_0);

        case State::Io:
            if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                remaining_->notify_one();
            }
            return done();
        }
        return failed();
    }

    State state_{State::Logic};
    std::atomic<int> *remaining_{nullptr};
};

class ParallelShardTask final : public Task {
public:
    explicit ParallelShardTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int> *remaining, std::atomic<std::uint64_t> *sum) {
        remaining_ = remaining;
        sum_ = sum;
        ops_ = af::ShardedOps<std::uint64_t>(4);
        for (std::uint64_t i = 0; i < 1024; ++i) {
            ops_.shards[i & 3U].push_back(i);
        }
        return schedule(BenchThreads::Logic_0);
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
            Runtime::parallel_shards(BenchThreads::Logic_0, ops_, af::ParallelMode::AllShards, this,
                                     [this](std::uint16_t, std::vector<std::uint64_t> &shard_ops) {
                                         std::uint64_t local = 0;
                                         for (auto value : shard_ops) {
                                             local += value;
                                         }
                                         sum_->fetch_add(local, std::memory_order_relaxed);
                                     });
            return pending();

        case State::Finish:
            if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                remaining_->notify_one();
            }
            return done();
        }

        return failed();
    }

    State state_{State::Split};
    af::ShardedOps<std::uint64_t> ops_{4};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<std::uint64_t> *sum_{nullptr};
};

} // namespace af_bench::runtime
