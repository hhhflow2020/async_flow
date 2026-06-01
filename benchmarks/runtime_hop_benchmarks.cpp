#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <cstddef>

#include <benchmark/benchmark.h>

namespace {

template <std::size_t TaskPoolRemoteReleaseBatchSize, bool TaskPoolCacheSlotIndex,
          std::size_t TaskPoolChunkSize = 256, std::size_t TaskPoolLocalCacheSetSize = 1,
          std::size_t TaskPoolDirectReleaseSetSize = 4, std::size_t TaskPoolLocalCacheCapacity = 64>
struct BatchRuntimeTraits {
    static constexpr auto threads = af_bench::runtime::BenchRuntimeTraits::threads;
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr std::size_t task_pool_remote_release_batch_size =
        TaskPoolRemoteReleaseBatchSize;
    static constexpr std::size_t task_pool_chunk_size = TaskPoolChunkSize;
    static constexpr bool task_pool_cache_slot_index = TaskPoolCacheSlotIndex;
    static constexpr std::size_t task_pool_local_cache_set_size = TaskPoolLocalCacheSetSize;
    static constexpr std::size_t task_pool_direct_release_set_size = TaskPoolDirectReleaseSetSize;
    static constexpr std::size_t task_pool_local_cache_capacity = TaskPoolLocalCacheCapacity;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

template <typename RuntimeT> class BatchHopTask final : public RuntimeT::Task {
    using Task = typename RuntimeT::Task;
    using Thread = typename RuntimeT::Thread;

public:
    explicit BatchHopTask(typename Task::FactoryToken token) : Task(token) {}

    bool do_it(int hops, std::atomic<int> *remaining) {
        hops_ = hops;
        remaining_ = remaining;
        return this->schedule(af_bench::runtime::BenchThreads::Logic_0);
    }

private:
    af::TaskResult run() override {
        if (hops_-- > 0) {
            const auto next = RuntimeT::current_thread() == af_bench::runtime::BenchThreads::Logic_0
                                  ? af_bench::runtime::BenchThreads::Logic_1
                                  : af_bench::runtime::BenchThreads::Logic_0;
            return this->pending_on(next);
        }

        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return this->done();
    }

    int hops_{0};
    std::atomic<int> *remaining_{nullptr};
};

void BM_RuntimeCrossThreadHop(benchmark::State &state) {
    af_bench::runtime::Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok =
                af_bench::runtime::Runtime::start_task<af_bench::runtime::HopTask>(8, &remaining);
            if (!ok) {
                af_bench::runtime::undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<HopTask> failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    af_bench::runtime::Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <std::size_t TaskPoolRemoteReleaseBatchSize, bool TaskPoolCacheSlotIndex,
          std::size_t TaskPoolChunkSize = 256, std::size_t TaskPoolLocalCacheSetSize = 1,
          std::size_t TaskPoolDirectReleaseSetSize = 4, std::size_t TaskPoolLocalCacheCapacity = 64>
void BM_RuntimeCrossThreadHopTaskPoolBatch(benchmark::State &state) {
    using Runtime = af::AsyncRuntime<BatchRuntimeTraits<
        TaskPoolRemoteReleaseBatchSize, TaskPoolCacheSlotIndex, TaskPoolChunkSize,
        TaskPoolLocalCacheSetSize, TaskPoolDirectReleaseSetSize, TaskPoolLocalCacheCapacity>>;

    Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok = Runtime::template start_task<BatchHopTask<Runtime>>(8, &remaining);
            if (!ok) {
                af_bench::runtime::undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<BatchHopTask> failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeIoThreadHop(benchmark::State &state) {
    af_bench::runtime::Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok =
                af_bench::runtime::Runtime::start_task<af_bench::runtime::IoHopTask>(&remaining);
            if (!ok) {
                af_bench::runtime::undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<IoHopTask> failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    af_bench::runtime::Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeCrossThreadHop)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 8, false)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 32, false)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false, 256, 8, 4, 64)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, true)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false, 256, 8, 4, 128)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false, 256, 1, 4, 128)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 128, false, 256, 1, 4, 128)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false, 512)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK_TEMPLATE(BM_RuntimeCrossThreadHopTaskPoolBatch, 64, false, 1024)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeIoThreadHop)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
