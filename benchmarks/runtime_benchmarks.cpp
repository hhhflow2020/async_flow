#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "af/async_flow.hpp"

namespace {

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

void wait_zero(std::atomic<int>& remaining) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        const int observed = remaining.load(std::memory_order_acquire);
        if (observed != 0) {
            remaining.wait(observed, std::memory_order_acquire);
        }
    }
}

void undo_remaining(std::atomic<int>& remaining) {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        remaining.notify_one();
    }
}

class CountTask final : public Task {
public:
    explicit CountTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(BenchThread thread, std::atomic<int>* remaining) {
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

    std::atomic<int>* remaining_{nullptr};
};

class HopTask final : public Task {
public:
    explicit HopTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int hops, std::atomic<int>* remaining) {
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
    std::atomic<int>* remaining_{nullptr};
};

class IoHopTask final : public Task {
public:
    explicit IoHopTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* remaining) {
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
    std::atomic<int>* remaining_{nullptr};
};

class ParallelShardTask final : public Task {
public:
    explicit ParallelShardTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::atomic<int>* remaining, std::atomic<std::uint64_t>* sum) {
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
            Runtime::parallel_shards(
                BenchThread::Logic_0,
                ops_,
                af::ParallelMode::AllShards,
                this,
                [this](std::uint16_t, std::vector<std::uint64_t>& shard_ops) {
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
    std::atomic<int>* remaining_{nullptr};
    std::atomic<std::uint64_t>* sum_{nullptr};
};

void BM_RuntimeExternalStart(benchmark::State& state) {
    Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            const auto thread = static_cast<BenchThread>(i & 3);
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok = Runtime::start_task<CountTask>(thread, &remaining);
            if (!ok) {
                undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<CountTask> failed");
                launch_failed = true;
                break;
            }
        }
        wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeCrossThreadHop(benchmark::State& state) {
    Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok = Runtime::start_task<HopTask>(8, &remaining);
            if (!ok) {
                undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<HopTask> failed");
                launch_failed = true;
                break;
            }
        }
        wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeIoThreadHop(benchmark::State& state) {
    Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok = Runtime::start_task<IoHopTask>(&remaining);
            if (!ok) {
                undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<IoHopTask> failed");
                launch_failed = true;
                break;
            }
        }
        wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeParallelShards(benchmark::State& state) {
    Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        std::atomic<std::uint64_t> sum{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok = Runtime::start_task<ParallelShardTask>(&remaining, &sum);
            if (!ok) {
                undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<ParallelShardTask> failed");
                launch_failed = true;
                break;
            }
        }
        wait_zero(remaining);
        benchmark::DoNotOptimize(sum.load(std::memory_order_relaxed));
        if (launch_failed) {
            break;
        }
    }
    Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeExternalStart)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHop)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeIoThreadHop)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeParallelShards)->Arg(128)->Arg(512)->Unit(benchmark::kMillisecond);

} // namespace
