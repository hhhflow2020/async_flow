#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "caf/caf.hpp"
#include "caf/detail/bounded_queues.hpp"
#include "caf/detail/object_pool.hpp"

namespace {

enum class BenchThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    enum_num_end,
};

struct BenchRuntimeTraits {
    using Thread = BenchThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(BenchThread::enum_num_end);
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr caf::QueueFullPolicy queue_full_policy = caf::QueueFullPolicy::Yield;
};

using Runtime = caf::AsyncRuntime<BenchRuntimeTraits>;
using Task = Runtime::Task;

void wait_zero(std::atomic<int>& remaining) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        const int observed = remaining.load(std::memory_order_acquire);
        if (observed != 0) {
            remaining.wait(observed, std::memory_order_acquire);
        }
    }
}

class CountTask final : public Task {
public:
    bool do_it(BenchThread thread, std::atomic<int>* remaining) {
        remaining_ = remaining;
        return schedule(thread);
    }

private:
    caf::TaskResult run() override {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    std::atomic<int>* remaining_{nullptr};
};

class HopTask final : public Task {
public:
    bool do_it(int hops, std::atomic<int>* remaining) {
        hops_ = hops;
        remaining_ = remaining;
        return schedule(BenchThread::Logic_0);
    }

private:
    caf::TaskResult run() override {
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

class ParallelShardTask final : public Task {
public:
    bool do_it(std::atomic<int>* remaining, std::atomic<std::uint64_t>* sum) {
        remaining_ = remaining;
        sum_ = sum;
        ops_ = caf::ShardedOps<std::uint64_t>(4);
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

    caf::TaskResult run() override {
        switch (state_) {
        case State::Split:
            state_ = State::Finish;
            Runtime::parallel_shards(
                BenchThread::Logic_0,
                ops_,
                caf::ParallelMode::AllShards,
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
    caf::ShardedOps<std::uint64_t> ops_{4};
    std::atomic<int>* remaining_{nullptr};
    std::atomic<std::uint64_t>* sum_{nullptr};
};

void BM_SpscQueuePushPop(benchmark::State& state) {
    caf::detail::BoundedSpscQueue<int> queue(65536);
    int value = 42;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            benchmark::DoNotOptimize(queue.try_push(&value));
            benchmark::DoNotOptimize(queue.try_pop());
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ObjectPoolCreateDestroy(benchmark::State& state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    caf::detail::ObjectPool<Payload> pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto* object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeExternalStart(benchmark::State& state) {
    for (auto _ : state) {
        Runtime::init();
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{task_count};
        for (int i = 0; i < task_count; ++i) {
            const auto thread = static_cast<BenchThread>(i & 3);
            benchmark::DoNotOptimize(Runtime::start_task<CountTask>(thread, &remaining));
        }
        wait_zero(remaining);
        Runtime::shutdown();
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeCrossThreadHop(benchmark::State& state) {
    for (auto _ : state) {
        Runtime::init();
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{task_count};
        for (int i = 0; i < task_count; ++i) {
            benchmark::DoNotOptimize(Runtime::start_task<HopTask>(8, &remaining));
        }
        wait_zero(remaining);
        Runtime::shutdown();
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeParallelShards(benchmark::State& state) {
    for (auto _ : state) {
        Runtime::init();
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{task_count};
        std::atomic<std::uint64_t> sum{0};
        for (int i = 0; i < task_count; ++i) {
            benchmark::DoNotOptimize(Runtime::start_task<ParallelShardTask>(&remaining, &sum));
        }
        wait_zero(remaining);
        benchmark::DoNotOptimize(sum.load(std::memory_order_relaxed));
        Runtime::shutdown();
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_SpscQueuePushPop)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolCreateDestroy)->Arg(1024)->Arg(16384);
BENCHMARK(BM_RuntimeExternalStart)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHop)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeParallelShards)->Arg(128)->Arg(512)->Unit(benchmark::kMillisecond);

} // namespace

BENCHMARK_MAIN();
