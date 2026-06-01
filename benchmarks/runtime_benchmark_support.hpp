#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/async_flow.hpp"

namespace af_bench::runtime {

enum class BenchThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    IO_0,
    enum_thread_index_end,
};

struct BenchRuntimeTraits {
    using Thread = BenchThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(BenchThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(BenchThread thread) noexcept {
        return thread == BenchThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using Runtime = af::AsyncRuntime<BenchRuntimeTraits>;
using Task = Runtime::Task;

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
        return schedule(BenchThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        if (hops_-- > 0) {
            const auto next = Runtime::current_thread() == BenchThread::Logic_0
                                  ? BenchThread::Logic_1
                                  : BenchThread::Logic_0;
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
        return schedule(BenchThread::Logic_0);
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
            return pending_on(BenchThread::IO_0);

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
        return schedule(BenchThread::Logic_0);
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
            Runtime::parallel_shards(BenchThread::Logic_0, ops_, af::ParallelMode::AllShards, this,
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
